package app

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type database struct {
	*sql.DB
	dialect string
}

type transaction struct {
	*sql.Tx
	dialect string
}

func bind(query string, dialect string) string {
	if dialect != "postgres" {
		return query
	}
	var b strings.Builder
	n := 1
	inSingle, inDouble, inLineComment, inBlockComment := false, false, false, false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if inLineComment {
			b.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}
		if inBlockComment {
			b.WriteByte(ch)
			if ch == '*' && i+1 < len(query) && query[i+1] == '/' {
				b.WriteByte('/')
				i++
				inBlockComment = false
			}
			continue
		}
		if !inSingle && !inDouble && ch == '-' && i+1 < len(query) && query[i+1] == '-' {
			b.WriteString("--")
			i++
			inLineComment = true
			continue
		}
		if !inSingle && !inDouble && ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			b.WriteString("/*")
			i++
			inBlockComment = true
			continue
		}
		if ch == '\'' && !inDouble {
			b.WriteByte(ch)
			if inSingle && i+1 < len(query) && query[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			b.WriteByte(ch)
			if inDouble && i+1 < len(query) && query[i+1] == '"' {
				b.WriteByte('"')
				i++
				continue
			}
			inDouble = !inDouble
			continue
		}
		if ch == '?' && !inSingle && !inDouble {
			fmt.Fprintf(&b, "$%d", n)
			n++
		} else {
			b.WriteByte(ch)
		}
	}
	return b.String()
}

func (d *database) Exec(query string, args ...any) (sql.Result, error) {
	return d.DB.Exec(bind(query, d.dialect), args...)
}
func (d *database) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.DB.ExecContext(ctx, bind(query, d.dialect), args...)
}
func (d *database) Query(query string, args ...any) (*sql.Rows, error) {
	return d.DB.Query(bind(query, d.dialect), args...)
}
func (d *database) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.DB.QueryContext(ctx, bind(query, d.dialect), args...)
}
func (d *database) QueryRow(query string, args ...any) *sql.Row {
	return d.DB.QueryRow(bind(query, d.dialect), args...)
}
func (d *database) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.DB.QueryRowContext(ctx, bind(query, d.dialect), args...)
}
func (d *database) Begin() (*transaction, error) {
	tx, err := d.DB.Begin()
	return &transaction{Tx: tx, dialect: d.dialect}, err
}
func (d *database) BeginTx(ctx context.Context, opts *sql.TxOptions) (*transaction, error) {
	tx, err := d.DB.BeginTx(ctx, opts)
	return &transaction{Tx: tx, dialect: d.dialect}, err
}
func (t *transaction) Exec(query string, args ...any) (sql.Result, error) {
	return t.Tx.Exec(bind(query, t.dialect), args...)
}
func (t *transaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.Tx.ExecContext(ctx, bind(query, t.dialect), args...)
}
func (t *transaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.Tx.QueryRowContext(ctx, bind(query, t.dialect), args...)
}

func insertID(query string, args ...any) (int64, error) {
	if db.dialect == "postgres" {
		var id int64
		err := db.QueryRow(query+" RETURNING id", args...).Scan(&id)
		return id, err
	}
	res, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
