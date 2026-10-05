package app

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"time"
)

//go:embed migrations/*.sql
var postgresMigrationFiles embed.FS

func applyPostgresMigrations() error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create PostgreSQL migrations table: %w", err)
	}
	entries, err := postgresMigrationFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("list PostgreSQL migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin PostgreSQL migration %s: %w", entry.Name(), err)
		}
		if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(248858713)`); err != nil {
			tx.Rollback()
			return fmt.Errorf("lock PostgreSQL migrations: %w", err)
		}
		var applied bool
		if err := tx.QueryRowContext(context.Background(), `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE id=?)`, entry.Name()).Scan(&applied); err != nil {
			tx.Rollback()
			return fmt.Errorf("check PostgreSQL migration %s: %w", entry.Name(), err)
		}
		if applied {
			tx.Rollback()
			continue
		}
		script, err := postgresMigrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("read PostgreSQL migration %s: %w", entry.Name(), err)
		}
		if _, err = tx.Exec(string(script)); err == nil {
			_, err = tx.Exec(`INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)`, entry.Name(), time.Now().UTC())
		}
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("apply PostgreSQL migration %s: %w", entry.Name(), err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit PostgreSQL migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}
