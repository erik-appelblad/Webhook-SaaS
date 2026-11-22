package store

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"testing/fstest"
)

type fakeMigrationTx struct {
	applied  map[string]string
	executed []string
	failOn   string
}

func newFakeMigrationTx() *fakeMigrationTx {
	return &fakeMigrationTx{
		applied: make(map[string]string),
	}
}

func (f *fakeMigrationTx) Exec(_ context.Context, sql string) error {
	if f.failOn != "" && strings.Contains(sql, f.failOn) {
		return errors.New("boom")
	}
	f.executed = append(f.executed, sql)
	return nil
}

func (f *fakeMigrationTx) AppliedMigrations(context.Context) (map[string]string, error) {
	copyMap := make(map[string]string, len(f.applied))
	for k, v := range f.applied {
		copyMap[k] = v
	}
	return copyMap, nil
}

func (f *fakeMigrationTx) RecordMigration(_ context.Context, version, checksum string) error {
	f.applied[version] = checksum
	return nil
}

func TestApplyMigrationsOrdersNumericallyAndIsIdempotent(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/010_tenth.sql":  {Data: []byte("CREATE TABLE t10 (id int);")},
		"migrations/001_init.sql":   {Data: []byte("CREATE TABLE t1 (id int);")},
		"migrations/002_second.sql": {Data: []byte("CREATE TABLE t2 (id int);")},
		"migrations/README.md":      {Data: []byte("ignored")},
	}
	tx := newFakeMigrationTx()

	if err := applyMigrations(context.Background(), tx, fsys, "migrations"); err != nil {
		t.Fatalf("first applyMigrations failed: %v", err)
	}

	if len(tx.executed) != 4 {
		t.Fatalf("expected 4 Exec calls (schema_migrations + 3 migrations), got %d", len(tx.executed))
	}
	if !strings.Contains(tx.executed[0], "CREATE TABLE IF NOT EXISTS schema_migrations") {
		t.Fatalf("expected schema_migrations creation first, got %q", tx.executed[0])
	}
	if tx.executed[1] != "CREATE TABLE t1 (id int);" ||
		tx.executed[2] != "CREATE TABLE t2 (id int);" ||
		tx.executed[3] != "CREATE TABLE t10 (id int);" {
		t.Fatalf("unexpected migration order: %v", tx.executed[1:])
	}
	if len(tx.applied) != 3 {
		t.Fatalf("expected 3 recorded migrations, got %d", len(tx.applied))
	}

	// Re-running should only ensure schema_migrations and skip all applied SQL files.
	tx.executed = nil
	if err := applyMigrations(context.Background(), tx, fsys, "migrations"); err != nil {
		t.Fatalf("second applyMigrations failed: %v", err)
	}
	if len(tx.executed) != 1 {
		t.Fatalf("expected only schema_migrations check on second run, got %d exec calls", len(tx.executed))
	}
}

func TestApplyMigrationsRejectsChecksumMismatch(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/001_init.sql": {Data: []byte("CREATE TABLE t1 (id int);")},
	}
	tx := newFakeMigrationTx()

	if err := applyMigrations(context.Background(), tx, fsys, "migrations"); err != nil {
		t.Fatalf("initial applyMigrations failed: %v", err)
	}

	fsys["migrations/001_init.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE t1 (id bigint);")}
	err := applyMigrations(context.Background(), tx, fsys, "migrations")
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
}

func TestApplyMigrationsRejectsInvalidFiles(t *testing.T) {
	cases := []struct {
		name string
		fsys fstest.MapFS
	}{
		{
			name: "missing numeric prefix",
			fsys: fstest.MapFS{
				"migrations/init.sql": {Data: []byte("SELECT 1;")},
			},
		},
		{
			name: "non-numeric prefix",
			fsys: fstest.MapFS{
				"migrations/abc_init.sql": {Data: []byte("SELECT 1;")},
			},
		},
		{
			name: "missing description after underscore",
			fsys: fstest.MapFS{
				"migrations/001_.sql": {Data: []byte("SELECT 1;")},
			},
		},
		{
			name: "duplicate migration number",
			fsys: fstest.MapFS{
				"migrations/002_first.sql":  {Data: []byte("SELECT 1;")},
				"migrations/002_second.sql": {Data: []byte("SELECT 2;")},
			},
		},
		{
			name: "empty migration sql",
			fsys: fstest.MapFS{
				"migrations/001_empty.sql": {Data: []byte("   \n\t ")},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := newFakeMigrationTx()
			if err := applyMigrations(context.Background(), tx, tc.fsys, "migrations"); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestApplyMigrationsDoesNotRecordOnSQLError(t *testing.T) {
	fsys := fstest.MapFS{
		"migrations/001_init.sql":   {Data: []byte("CREATE TABLE t1 (id int);")},
		"migrations/002_broken.sql": {Data: []byte("BROKEN SQL;")},
	}
	tx := newFakeMigrationTx()
	tx.failOn = "BROKEN SQL"

	err := applyMigrations(context.Background(), tx, fsys, "migrations")
	if err == nil {
		t.Fatal("expected migration error, got nil")
	}
	if _, ok := tx.applied["001_init.sql"]; !ok {
		t.Fatal("expected 001_init.sql to be recorded")
	}
	if _, ok := tx.applied["002_broken.sql"]; ok {
		t.Fatal("broken migration should not be recorded")
	}
}

func TestLoadEmbeddedMigrations(t *testing.T) {
	loaded, err := loadMigrations(migrationFiles, "migrations")
	if err != nil {
		t.Fatalf("loadMigrations on embedded FS failed: %v", err)
	}
	if len(loaded) == 0 || loaded[0].Version != "001_init.sql" {
		t.Fatalf("expected 001_init.sql in embedded migrations, got %+v", loaded)
	}
}

func TestStoreMigratePostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run Postgres migration test")
	}

	ctx := context.Background()
	database, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer database.Close()

	if err := database.Migrate(ctx); err != nil {
		t.Fatalf("first Migrate failed: %v", err)
	}
	if err := database.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate failed: %v", err)
	}
}
