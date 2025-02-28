package sqlreader

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// migrationManager handles database migrations.
// It's responsible for loading migration files, tracking applied migrations,
// and applying or rolling back migrations as needed.
type migrationManager struct {
	db            dbConn
	queries       embed.FS
	migrationsDir string
	logger        Logger
	metrics       MetricsCollector
}

// migration represents a single database migration.
// Each migration has a version number, name, up and down SQL statements,
// and a timestamp indicating when it was applied.
type migration struct {
	Version   int       // The migration version number (used for sorting)
	Name      string    // A descriptive name for the migration
	UpSQL     string    // SQL to apply the migration
	DownSQL   string    // SQL to revert the migration
	AppliedAt time.Time // When the migration was applied
}

// newMigrationManager creates a new migration manager with the given
// database connection, embedded filesystem, and migrations directory.
func newMigrationManager(db dbConn, queries embed.FS, migrationsDir string) *migrationManager {
	return &migrationManager{
		db:            db,
		queries:       queries,
		migrationsDir: migrationsDir,
		logger:        defaultLogger,
		metrics:       defaultMetricsCollector,
	}
}

// Initialize creates the migrations table if it doesn't exist.
// This table is used to track which migrations have been applied.
func (m *migrationManager) Initialize(ctx context.Context) error {
	logger := LoggerFromContext(ctx)
	logger.Debug("Initializing migrations table")

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  TIMESTAMP WITH TIME ZONE NOT NULL
		);
	`

	startTime := time.Now()
	_, err := m.db.Exec(ctx, createTableSQL)
	duration := time.Since(startTime)

	if err != nil {
		logger.Error("Failed to create migrations table",
			"error", err,
			"duration_ms", duration.Milliseconds())

		metrics := MetricsFromContext(ctx)
		metrics.IncrementError("init_migrations_table")

		return fmt.Errorf("creating migrations table: %w", err)
	}

	logger.Debug("Migrations table initialized successfully",
		"duration_ms", duration.Milliseconds())

	return nil
}

// LoadMigrations loads all migrations from the embedded filesystem.
// Migration files are expected to be named in the format "001_create_users.sql"
// where "001" is the version number and "create_users" is the name.
// Each file should contain up SQL followed by a "-- Down" separator and down SQL.
func (m *migrationManager) LoadMigrations() ([]migration, error) {
	m.logger.Debug("Loading migrations from filesystem",
		"migrationsDir", m.migrationsDir)

	entries, err := m.queries.ReadDir(m.migrationsDir)
	if err != nil {
		m.logger.Error("Failed to read migrations directory",
			"error", err,
			"dir", m.migrationsDir)

		m.metrics.IncrementError("read_migrations_dir")
		return nil, fmt.Errorf("reading migrations directory: %w", err)
	}

	var migrations []migration
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			m.logger.Debug("Reading migration file", "filename", entry.Name())

			content, err := m.queries.ReadFile(m.migrationsDir + "/" + entry.Name())
			if err != nil {
				m.logger.Error("Failed to read migration file",
					"error", err,
					"filename", entry.Name())

				m.metrics.IncrementError("read_migration_file")
				return nil, fmt.Errorf("reading migration file %s: %w", entry.Name(), err)
			}

			parts := strings.Split(strings.TrimSuffix(entry.Name(), ".sql"), "_")
			if len(parts) < 2 {
				m.logger.Error("Invalid migration filename format",
					"filename", entry.Name(),
					"expected", "VERSION_NAME.sql")

				m.metrics.IncrementError("invalid_migration_filename")
				return nil, fmt.Errorf("invalid migration filename: %s", entry.Name())
			}

			version := 0
			_, err = fmt.Sscanf(parts[0], "%d", &version)
			if err != nil {
				m.logger.Error("Failed to parse migration version",
					"error", err,
					"filename", entry.Name(),
					"version_part", parts[0])

				m.metrics.IncrementError("parse_migration_version")
				return nil, fmt.Errorf("parsing migration version from %s: %w", entry.Name(), err)
			}

			name := strings.Join(parts[1:], "_")
			sections := strings.Split(string(content), "-- Down")
			if len(sections) != 2 {
				m.logger.Error("Invalid migration format, missing '-- Down' separator",
					"filename", entry.Name())

				m.metrics.IncrementError("invalid_migration_format")
				return nil, fmt.Errorf("invalid migration format in %s", entry.Name())
			}

			upSQL := strings.TrimSpace(sections[0])
			downSQL := strings.TrimSpace(sections[1])

			migrations = append(migrations, migration{
				Version: version,
				Name:    name,
				UpSQL:   upSQL,
				DownSQL: downSQL,
			})

			m.logger.Debug("Loaded migration",
				"version", version,
				"name", name)
		}
	}

	// Sort migrations by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	m.logger.Info("Loaded migrations", "count", len(migrations))
	return migrations, nil
}

// GetAppliedMigrations returns all migrations that have been applied.
// It queries the schema_migrations table and returns a map of version to migration.
func (m *migrationManager) GetAppliedMigrations(ctx context.Context) (map[int]migration, error) {
	logger := LoggerFromContext(ctx)
	logger.Debug("Getting applied migrations")

	startTime := time.Now()
	rows, err := m.db.Query(ctx, `
		SELECT version, name, applied_at
		FROM schema_migrations
		ORDER BY version ASC
	`)

	if err != nil {
		duration := time.Since(startTime)
		logger.Error("Failed to query applied migrations",
			"error", err,
			"duration_ms", duration.Milliseconds())

		metrics := MetricsFromContext(ctx)
		metrics.IncrementError("query_applied_migrations")

		return nil, fmt.Errorf("querying applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]migration)
	for rows.Next() {
		var mig migration
		err := rows.Scan(&mig.Version, &mig.Name, &mig.AppliedAt)
		if err != nil {
			logger.Error("Failed to scan migration row", "error", err)

			metrics := MetricsFromContext(ctx)
			metrics.IncrementError("scan_migration_row")

			return nil, fmt.Errorf("scanning migration row: %w", err)
		}
		applied[mig.Version] = mig

		logger.Debug("Found applied migration",
			"version", mig.Version,
			"name", mig.Name,
			"applied_at", mig.AppliedAt)
	}

	duration := time.Since(startTime)
	logger.Debug("Retrieved applied migrations",
		"count", len(applied),
		"duration_ms", duration.Milliseconds())

	return applied, rows.Err()
}

// Migrate applies all pending migrations.
// It first loads all migrations from the filesystem, then checks which ones
// have already been applied. It then applies any migrations that haven't been
// applied yet, in order of version number.
func (m *migrationManager) Migrate(ctx context.Context) error {
	logger := LoggerFromContext(ctx)
	metrics := MetricsFromContext(ctx)

	logger.Debug("Starting migration process")

	if err := m.Initialize(ctx); err != nil {
		return err
	}

	migrations, err := m.LoadMigrations()
	if err != nil {
		return err
	}

	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	tx, ok := m.db.(pgx.Tx)
	if !ok {
		logger.Error("Database connection is not a transaction")
		metrics.IncrementError("migrate_not_transaction")
		return fmt.Errorf("database connection is not a transaction")
	}

	appliedCount := 0
	for _, migration := range migrations {
		if _, exists := applied[migration.Version]; !exists {
			// Apply migration
			logger.Info("Applying migration",
				"version", migration.Version,
				"name", migration.Name)

			startTime := time.Now()
			_, err := tx.Exec(ctx, migration.UpSQL)
			duration := time.Since(startTime)

			success := err == nil
			metrics.ObserveMigration(migration.Version, migration.Name, duration, success)

			if err != nil {
				logger.Error("Failed to apply migration",
					"version", migration.Version,
					"name", migration.Name,
					"error", err,
					"duration_ms", duration.Milliseconds())

				metrics.IncrementError("apply_migration")
				return fmt.Errorf("applying migration %d: %w", migration.Version, err)
			}

			logger.Debug("Migration applied successfully",
				"version", migration.Version,
				"name", migration.Name,
				"duration_ms", duration.Milliseconds())

			// Record migration
			appliedTime := time.Now().UTC()
			_, err = tx.Exec(ctx, `
				INSERT INTO schema_migrations (version, name, applied_at)
				VALUES ($1, $2, $3)
			`, migration.Version, migration.Name, appliedTime)

			if err != nil {
				logger.Error("Failed to record migration",
					"version", migration.Version,
					"name", migration.Name,
					"error", err)

				metrics.IncrementError("record_migration")
				return fmt.Errorf("recording migration %d: %w", migration.Version, err)
			}

			logger.Debug("Migration recorded in schema_migrations",
				"version", migration.Version,
				"name", migration.Name)

			appliedCount++
		}
	}

	logger.Info("Migration completed", "applied", appliedCount, "total", len(migrations))
	return nil
}

// Rollback reverts the last applied migration.
// It first determines which migration was applied last, then executes
// the down SQL for that migration and removes the record from the
// schema_migrations table.
func (m *migrationManager) Rollback(ctx context.Context) error {
	logger := LoggerFromContext(ctx)
	metrics := MetricsFromContext(ctx)

	logger.Debug("Starting rollback process")

	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	if len(applied) == 0 {
		logger.Info("No migrations to roll back")
		return nil
	}

	migrations, err := m.LoadMigrations()
	if err != nil {
		return err
	}

	var lastMigration migration
	var lastVersion int
	for version := range applied {
		if version > lastVersion {
			lastVersion = version
			for _, m := range migrations {
				if m.Version == version {
					lastMigration = m
					break
				}
			}
		}
	}

	tx, ok := m.db.(pgx.Tx)
	if !ok {
		logger.Error("Database connection is not a transaction")
		metrics.IncrementError("rollback_not_transaction")
		return fmt.Errorf("database connection is not a transaction")
	}

	// Apply rollback
	logger.Info("Rolling back migration",
		"version", lastMigration.Version,
		"name", lastMigration.Name)

	startTime := time.Now()
	_, err = tx.Exec(ctx, lastMigration.DownSQL)
	duration := time.Since(startTime)

	success := err == nil
	metrics.ObserveMigration(lastMigration.Version, lastMigration.Name, duration, success)

	if err != nil {
		logger.Error("Failed to roll back migration",
			"version", lastMigration.Version,
			"name", lastMigration.Name,
			"error", err,
			"duration_ms", duration.Milliseconds())

		metrics.IncrementError("rollback_migration")
		return fmt.Errorf("rolling back migration %d: %w", lastMigration.Version, err)
	}

	logger.Debug("Migration rolled back successfully",
		"version", lastMigration.Version,
		"name", lastMigration.Name,
		"duration_ms", duration.Milliseconds())

	// Remove migration record
	_, err = tx.Exec(ctx, `
		DELETE FROM schema_migrations
		WHERE version = $1
	`, lastMigration.Version)

	if err != nil {
		logger.Error("Failed to remove migration record",
			"version", lastMigration.Version,
			"name", lastMigration.Name,
			"error", err)

		metrics.IncrementError("remove_migration_record")
		return fmt.Errorf("removing migration record %d: %w", lastMigration.Version, err)
	}

	logger.Debug("Migration record removed from schema_migrations",
		"version", lastMigration.Version,
		"name", lastMigration.Name)

	logger.Info("Rollback completed successfully")
	return nil
}
