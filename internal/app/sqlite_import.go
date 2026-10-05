package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

var importTableOrder = []string{"projects", "builds", "runners", "jobs", "build_annotations", "mcp_idempotency", "runner_telemetry", "runner_operations"}
var sqlIdentifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

type sqlQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// ImportSQLite copies a stopped SQLite Deployer database into an empty,
// migrated PostgreSQL database. The source is opened read-only and never changed.
func ImportSQLite(ctx context.Context, sqlitePath, postgresURL string, out io.Writer) error {
	if strings.TrimSpace(sqlitePath) == "" {
		return fmt.Errorf("SQLite source path is required")
	}
	if strings.TrimSpace(postgresURL) == "" {
		return fmt.Errorf("DEPLOYER_DATABASE_URL is required")
	}
	source, err := sql.Open("sqlite", "file:"+sqlitePath+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open SQLite source: %w", err)
	}
	defer source.Close()
	if err := source.PingContext(ctx); err != nil {
		return fmt.Errorf("read SQLite source: %w", err)
	}
	sourceTx, err := source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin SQLite snapshot: %w", err)
	}
	defer sourceTx.Rollback()
	if err := rejectUnknownSQLiteTables(ctx, sourceTx); err != nil {
		return err
	}
	for _, table := range []string{"projects", "builds", "runners", "jobs"} {
		exists, err := sqliteTableExists(ctx, sourceTx, table)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("SQLite source is not a Deployer database: missing %s", table)
		}
	}

	targetSQL, err := sql.Open("pgx", postgresURL)
	if err != nil {
		return fmt.Errorf("open PostgreSQL target: %w", err)
	}
	defer targetSQL.Close()
	target := &database{DB: targetSQL, dialect: "postgres"}
	oldDB := db
	db = target
	defer func() { db = oldDB }()
	if err := target.PingContext(ctx); err != nil {
		return fmt.Errorf("connect PostgreSQL target: %w", err)
	}
	if err := applyPostgresMigrations(); err != nil {
		return err
	}

	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin import: %w", err)
	}
	defer tx.Rollback()
	for _, table := range []string{"notification_channels", "notification_deliveries"} {
		var count int64
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
			return fmt.Errorf("count target %s: %w", table, err)
		}
		if count != 0 {
			return fmt.Errorf("PostgreSQL target table %s is not empty", table)
		}
	}
	for _, table := range importTableOrder {
		var targetCount int64
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&targetCount); err != nil {
			return fmt.Errorf("count target %s: %w", table, err)
		}
		if targetCount != 0 {
			return fmt.Errorf("PostgreSQL target table %s is not empty", table)
		}
		exists, err := sqliteTableExists(ctx, sourceTx, table)
		if err != nil {
			return err
		}
		if !exists {
			continue
		}
		count, sourceDigest, columns, err := copySQLiteTable(ctx, sourceTx, tx, table)
		if err != nil {
			return err
		}
		targetDigest, err := tableDigest(ctx, tx, table, columns)
		if err != nil {
			return fmt.Errorf("verify %s contents: %w", table, err)
		}
		if sourceDigest != targetDigest {
			return fmt.Errorf("verify %s contents: digest mismatch", table)
		}
		fmt.Fprintf(out, "%s: %d records verified\n", table, count)
	}
	if _, err := tx.ExecContext(ctx, `SET CONSTRAINTS ALL IMMEDIATE`); err != nil {
		return fmt.Errorf("verify relationships: %w", err)
	}
	for _, check := range []struct{ name, query string }{
		{"build projects", `SELECT COUNT(*) FROM builds b LEFT JOIN projects p ON p.id=b.project_id WHERE p.id IS NULL`},
		{"job builds/runners", `SELECT COUNT(*) FROM jobs j LEFT JOIN builds b ON b.id=j.build_id LEFT JOIN runners r ON r.id=j.runner_id WHERE b.id IS NULL OR r.id IS NULL`},
		{"annotation builds", `SELECT COUNT(*) FROM build_annotations a LEFT JOIN builds b ON b.id=a.build_id WHERE b.id IS NULL`},
	} {
		var invalid int64
		if err := tx.QueryRowContext(ctx, check.query).Scan(&invalid); err != nil {
			return fmt.Errorf("verify %s: %w", check.name, err)
		}
		if invalid != 0 {
			return fmt.Errorf("verify %s: %d broken relationships", check.name, invalid)
		}
	}
	for _, table := range []string{"projects", "builds", "runners", "jobs", "build_annotations", "runner_operations"} {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT setval(pg_get_serial_sequence('%s','id'), COALESCE((SELECT MAX(id) FROM %s), 1), (SELECT COUNT(*) > 0 FROM %s))`, table, table, table)); err != nil {
			return fmt.Errorf("reset %s identity: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit import: %w", err)
	}
	if err := sourceTx.Commit(); err != nil {
		return fmt.Errorf("finish SQLite snapshot: %w", err)
	}
	return nil
}

func rejectUnknownSQLiteTables(ctx context.Context, source sqlQueryer) error {
	rows, err := source.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return fmt.Errorf("list SQLite tables: %w", err)
	}
	defer rows.Close()
	known := map[string]bool{"schema_migrations": true}
	for _, table := range importTableOrder {
		known[table] = true
	}
	var unknown []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if !known[name] {
			unknown = append(unknown, name)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(unknown) > 0 {
		return fmt.Errorf("SQLite contains unsupported persisted tables: %s; refusing incomplete import", strings.Join(unknown, ", "))
	}
	return nil
}

func sqliteTableExists(ctx context.Context, source sqlQueryer, table string) (bool, error) {
	var exists bool
	err := source.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)`, table).Scan(&exists)
	return exists, err
}

func copySQLiteTable(ctx context.Context, source sqlQueryer, tx *transaction, table string) (int64, [32]byte, []string, error) {
	var zero [32]byte
	rows, err := source.QueryContext(ctx, "SELECT * FROM "+table)
	if err != nil {
		return 0, zero, nil, fmt.Errorf("read SQLite %s: %w", table, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return 0, zero, nil, err
	}
	sort.Strings(columns) // deterministic import and diagnostics
	rows.Close()
	for _, column := range columns {
		if !sqlIdentifier.MatchString(column) {
			return 0, zero, nil, fmt.Errorf("unsafe SQLite column name %q", column)
		}
	}
	query := "SELECT " + strings.Join(columns, ",") + " FROM " + table
	rows, err = source.QueryContext(ctx, query)
	if err != nil {
		return 0, zero, nil, err
	}
	defer rows.Close()
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",")
	insert := "INSERT INTO " + table + " (" + strings.Join(columns, ",") + ") VALUES (" + placeholders + ")"
	var count int64
	canonical := []string{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return 0, zero, nil, fmt.Errorf("scan SQLite %s: %w", table, err)
		}
		for i, column := range columns {
			if (table == "projects" && column == "git_pull_before_build") || (table == "runner_telemetry" && column == "docker_available") {
				switch v := values[i].(type) {
				case int64:
					values[i] = v != 0
				case []byte:
					values[i] = string(v) != "0"
				}
			}
		}
		canonical = append(canonical, canonicalRow(table, columns, values))
		if _, err := tx.ExecContext(ctx, insert, values...); err != nil {
			return 0, zero, nil, fmt.Errorf("import SQLite %s row %d: %w", table, count+1, err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, zero, nil, err
	}
	var copied int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&copied); err != nil {
		return 0, zero, nil, err
	}
	if copied != count {
		return 0, zero, nil, fmt.Errorf("verify %s count: source=%d target=%d", table, count, copied)
	}
	sort.Strings(canonical)
	return count, sha256.Sum256([]byte(strings.Join(canonical, "\n"))), columns, nil
}

func tableDigest(ctx context.Context, source sqlQueryer, table string, columns []string) ([32]byte, error) {
	var zero [32]byte
	rows, err := source.QueryContext(ctx, "SELECT "+strings.Join(columns, ",")+" FROM "+table)
	if err != nil {
		return zero, err
	}
	defer rows.Close()
	canonical := []string{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return zero, err
		}
		canonical = append(canonical, canonicalRow(table, columns, values))
	}
	if err := rows.Err(); err != nil {
		return zero, err
	}
	sort.Strings(canonical)
	return sha256.Sum256([]byte(strings.Join(canonical, "\n"))), nil
}

func canonicalRow(table string, columns []string, values []any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			parts[i] = "<null>"
			continue
		}
		if b, ok := v.([]byte); ok {
			v = string(b)
		}
		if t, ok := v.(time.Time); ok {
			parts[i] = t.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
			continue
		}
		if (table == "projects" && columns[i] == "git_pull_before_build") || (table == "runner_telemetry" && columns[i] == "docker_available") {
			switch x := v.(type) {
			case bool:
				parts[i] = fmt.Sprint(x)
			case int64:
				parts[i] = fmt.Sprint(x != 0)
			default:
				parts[i] = fmt.Sprint(v)
			}
			continue
		}
		s := fmt.Sprint(v)
		if strings.Contains(columns[i], "_at") {
			if parsed := parseSQLiteTime(s); !parsed.IsZero() {
				s = parsed.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
			}
		}
		parts[i] = s
	}
	return strings.Join(parts, "\x00")
}
