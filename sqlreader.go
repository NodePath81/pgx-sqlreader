package sqlreader

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLReader is the main interface for the SQLReader package
type SQLReader struct {
	queries       *queryStore
	migrations    *migrationManager
	queriesFS     embed.FS
	queriesDir    string
	migrationsDir string
}

// New creates a new SQLReader instance
func New(queriesFS embed.FS, queriesDir, migrationsDir string) (*SQLReader, error) {
	queries, err := newQueryStore(queriesFS, queriesDir)
	if err != nil {
		return nil, fmt.Errorf("initializing query store: %w", err)
	}

	return &SQLReader{
		queries:       queries,
		queriesFS:     queriesFS,
		queriesDir:    queriesDir,
		migrationsDir: migrationsDir,
	}, nil
}

// GetSQL retrieves an SQL query by name
// Panics if the query is not found
func (r *SQLReader) GetSQL(name string) string {
	return r.queries.get(name)
}

// Connector wraps a database connection with query execution methods
type Connector struct {
	db     dbConn
	reader *SQLReader
	loader *queryLoader
}

// ConnectPool creates a new connector from a database connection pool
func (r *SQLReader) ConnectPool(pool *pgxpool.Pool) *Connector {
	loader := &queryLoader{
		db:      pool,
		querier: r.queries,
	}

	return &Connector{
		db:     pool,
		reader: r,
		loader: loader,
	}
}

// ConnectTx creates a new connector from a database transaction
func (r *SQLReader) ConnectTx(tx pgx.Tx) *Connector {
	loader := &queryLoader{
		db:      tx,
		querier: r.queries,
	}

	return &Connector{
		db:     tx,
		reader: r,
		loader: loader,
	}
}

// Exec executes a named SQL query that doesn't return any rows
func (c *Connector) Exec(ctx context.Context, name string, args ...interface{}) error {
	return c.loader.exec(ctx, name, args...)
}

// QueryRow executes a named SQL query that returns a single row
func (c *Connector) QueryRow(ctx context.Context, name string, scanner func(pgx.Row) error, args ...interface{}) error {
	return c.loader.queryRow(ctx, name, scanner, args...)
}

// QueryRows executes a named SQL query that returns multiple rows
func (c *Connector) QueryRows(ctx context.Context, name string, scanner func(pgx.Rows) error, args ...interface{}) error {
	return c.loader.queryRows(ctx, name, scanner, args...)
}

// InitiateMigration initializes the migration manager and ensures the migrations table exists
func (c *Connector) InitiateMigration(ctx context.Context) error {
	c.reader.migrations = newMigrationManager(c.db, c.reader.queriesFS, c.reader.migrationsDir)
	return c.reader.migrations.Initialize(ctx)
}

// Migrate applies all pending migrations
func (c *Connector) Migrate(ctx context.Context) error {
	// Initialize migration manager if needed
	if c.reader.migrations == nil {
		if err := c.InitiateMigration(ctx); err != nil {
			return err
		}
	}

	// Check if we're already in a transaction
	_, isTx := c.db.(pgx.Tx)
	if isTx {
		// Already in a transaction, just migrate
		return c.reader.migrations.Migrate(ctx)
	}

	// Need to start a transaction for migration
	conn, ok := c.db.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("unexpected connection type, expected *pgxpool.Pool")
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting transaction for migration: %w", err)
	}

	// Create a new migration manager with the transaction
	txMigrations := newMigrationManager(tx, c.reader.queriesFS, c.reader.migrationsDir)

	// Apply migrations
	if err := txMigrations.Migrate(ctx); err != nil {
		tx.Rollback(ctx)
		return err
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing migration transaction: %w", err)
	}

	return nil
}

// Rollback reverts the last applied migration
func (c *Connector) Rollback(ctx context.Context) error {
	if c.reader.migrations == nil {
		if err := c.InitiateMigration(ctx); err != nil {
			return err
		}
	}

	// Check if we're already in a transaction
	_, isTx := c.db.(pgx.Tx)
	if isTx {
		// Already in a transaction, just rollback
		return c.reader.migrations.Rollback(ctx)
	}

	// Need to start a transaction for rollback
	conn, ok := c.db.(*pgxpool.Pool)
	if !ok {
		return fmt.Errorf("unexpected connection type, expected *pgxpool.Pool")
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting transaction for rollback: %w", err)
	}

	// Create a new migration manager with the transaction
	txMigrations := newMigrationManager(tx, c.reader.queriesFS, c.reader.migrationsDir)

	// Apply rollback
	if err := txMigrations.Rollback(ctx); err != nil {
		tx.Rollback(ctx)
		return err
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing rollback transaction: %w", err)
	}

	return nil
}

// ExecuteJSONBQuery executes a query with JSONB support
// This is a convenience method for working with JSONB columns
func (c *Connector) ExecuteJSONBQuery(ctx context.Context, name string, scanner func(pgx.Rows) error, args ...interface{}) error {
	return c.loader.queryRows(ctx, name, scanner, args...)
}

// Some helper methods for common JSONB operations

// JSONBContains generates a query fragment for JSONB containment (@>)
func JSONBContains(column string, value interface{}) string {
	return fmt.Sprintf("%s @> $1", column)
}

// JSONBContainedBy generates a query fragment for JSONB contained by (<@)
func JSONBContainedBy(column string, value interface{}) string {
	return fmt.Sprintf("%s <@ $1", column)
}

// JSONBHasKey generates a query fragment to check if a JSONB document has a key
func JSONBHasKey(column, key string) string {
	return fmt.Sprintf("%s ? %s", column, key)
}

// JSONBGetPath generates a query fragment to extract a value at a specified path
func JSONBGetPath(column string, path ...string) string {
	if len(path) == 0 {
		return column
	}

	pathExpr := column
	for _, p := range path {
		pathExpr = fmt.Sprintf("%s->>'%s'", pathExpr, p)
	}
	return pathExpr
}
