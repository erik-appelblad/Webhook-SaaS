package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

const migrationLockID int64 = 734219831

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migration represents a single versioned SQL migration file.
type Migration struct {
	Number   int
	Version  string
	SQL      string
	Checksum string
}

type migrationTx interface {
	Exec(ctx context.Context, sql string) error
	AppliedMigrations(ctx context.Context) (map[string]string, error)
	RecordMigration(ctx context.Context, version, checksum string) error
}

type pgxMigrationTx struct {
	tx pgx.Tx
}

func (m *pgxMigrationTx) Exec(ctx context.Context, sql string) error {
	// Pass no query arguments so pgx executes via the simple protocol,
	// allowing multi-statement migration scripts.
	_, err := m.tx.Exec(ctx, sql)
	return err
}

func (m *pgxMigrationTx) AppliedMigrations(ctx context.Context) (map[string]string, error) {
	rows, err := m.tx.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[string]string)
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, err
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return applied, nil
}

func (m *pgxMigrationTx) RecordMigration(ctx context.Context, version, checksum string) error {
	_, err := m.tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, checksum)
		VALUES ($1, $2)
	`, version, checksum)
	return err
}

// Migrate applies all pending embedded SQL migrations inside a locked transaction.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op if committed

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	if err := applyMigrations(ctx, &pgxMigrationTx{tx: tx}, migrationFiles, "migrations"); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

func applyMigrations(ctx context.Context, tx migrationTx, fsys fs.FS, dir string) error {
	migrations, err := loadMigrations(fsys, dir)
	if err != nil {
		return err
	}

	if err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	applied, err := tx.AppliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}

	for _, migration := range migrations {
		if appliedChecksum, ok := applied[migration.Version]; ok {
			if appliedChecksum != migration.Checksum {
				return fmt.Errorf(
					"migration %s checksum mismatch: applied %s, file %s",
					migration.Version,
					appliedChecksum,
					migration.Checksum,
				)
			}
			continue
		}

		if err := tx.Exec(ctx, migration.SQL); err != nil {
			return fmt.Errorf("apply migration %s: %w", migration.Version, err)
		}
		if err := tx.RecordMigration(ctx, migration.Version, migration.Checksum); err != nil {
			return fmt.Errorf("record migration %s: %w", migration.Version, err)
		}
	}

	return nil
}

func loadMigrations(fsys fs.FS, dir string) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations directory %q: %w", dir, err)
	}

	migrations := make([]Migration, 0, len(entries))
	seenNumbers := make(map[int]string)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}

		number, err := parseMigrationNumber(name)
		if err != nil {
			return nil, err
		}
		if existing, duplicate := seenNumbers[number]; duplicate {
			return nil, fmt.Errorf("duplicate migration number %d in %q and %q", number, existing, name)
		}
		seenNumbers[number] = name

		raw, err := fs.ReadFile(fsys, path.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", name, err)
		}
		sqlText := string(raw)
		if strings.TrimSpace(sqlText) == "" {
			return nil, fmt.Errorf("migration %s is empty", name)
		}

		sum := sha256.Sum256(raw)
		migrations = append(migrations, Migration{
			Number:   number,
			Version:  name,
			SQL:      sqlText,
			Checksum: hex.EncodeToString(sum[:]),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Number < migrations[j].Number
	})

	return migrations, nil
}

func parseMigrationNumber(filename string) (int, error) {
	prefix, rest, ok := strings.Cut(filename, "_")
	if !ok || prefix == "" || strings.TrimSuffix(rest, ".sql") == "" {
		return 0, fmt.Errorf("invalid migration filename %q: expected <number>_<name>.sql", filename)
	}
	for i := 0; i < len(prefix); i++ {
		if prefix[i] < '0' || prefix[i] > '9' {
			return 0, fmt.Errorf("invalid migration filename %q: expected numeric prefix", filename)
		}
	}
	number, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("invalid migration number in %q: %w", filename, err)
	}
	return number, nil
}
