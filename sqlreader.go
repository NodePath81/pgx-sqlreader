// Package sqlreader provides a PostgreSQL SQL file reader and migration manager
// for Go applications that use the pgx driver.
//
// It loads SQL queries at compile time using Go's embed package, parses named queries,
// and provides a convenient API for executing queries and managing migrations.
//
// Basic usage:
//
//	//go:embed sql/*.sql
//	//go:embed migrations/*.sql
//	var embeddedFiles embed.FS
//
//	reader, err := sqlreader.New(embeddedFiles, "sql", "migrations")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	pool, err := pgxpool.New(context.Background(), "postgres://postgres:password@localhost/mydb")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer pool.Close()
//
//	conn := reader.ConnectPool(pool)
//
//	// Apply migrations
//	if err := conn.Migrate(context.Background()); err != nil {
//	    log.Fatal(err)
//	}
//
//	// Execute a query
//	err = conn.Exec(context.Background(), "create_user", "john", "John Doe")
package sqlreader

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQLReader is the main interface for the SQLReader package.
// It holds loaded SQL queries and provides methods to create database connections.
type SQLReader struct {
	queries       *queryStore
	migrations    *migrationManager
	queriesFS     embed.FS
	queriesDir    string
	migrationsDir string
}

// New creates a new SQLReader instance.
//
// Parameters:
//   - queriesFS: An embedded filesystem containing SQL queries and migrations
//   - queriesDir: The directory in the filesystem containing SQL query files
//   - migrationsDir: The directory in the filesystem containing migration files
//
// Returns a new SQLReader instance or an error if initialization fails.
//
// Example:
//
//	//go:embed sql/*.sql migrations/*.sql
//	var fs embed.FS
//
//	reader, err := sqlreader.New(fs, "sql", "migrations")
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

// GetSQL retrieves an SQL query by name.
// Panics if the query is not found.
//
// This method is useful when you need to get the raw SQL text of a query,
// for example when using it with a different database library.
//
// Example:
//
//	sql := reader.GetSQL("get_user_by_id")
//	fmt.Println(sql) // SELECT * FROM users WHERE id = $1
func (r *SQLReader) GetSQL(name string) string {
	return r.queries.get(name)
}

// Connector wraps a database connection with query execution methods.
// It provides a convenient API for executing queries and managing migrations.
type Connector struct {
	db     dbConn
	reader *SQLReader
	loader *queryLoader
}

// ConnectPool creates a new connector from a database connection pool.
//
// This is the recommended way to create a connector for most applications,
// especially those that need to handle multiple concurrent requests.
//
// Example:
//
//	pool, _ := pgxpool.New(context.Background(), "postgres://postgres:password@localhost/mydb")
//	defer pool.Close()
//
//	conn := reader.ConnectPool(pool)
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

// ConnectTx creates a new connector from a database transaction.
//
// This is useful when you need to execute multiple queries in a transaction,
// for example when implementing a complex business operation that needs to be atomic.
//
// Example:
//
//	tx, _ := pool.Begin(context.Background())
//	defer tx.Rollback(context.Background())
//
//	txConn := reader.ConnectTx(tx)
//	// Execute queries in transaction
//	tx.Commit(context.Background())
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

// Exec executes a named SQL query that doesn't return any rows.
//
// Parameters:
//   - ctx: The context for the query execution
//   - name: The name of the query to execute
//   - args: Arguments for the query placeholders
//
// Example:
//
//	// Execute a query to create a user
//	err := conn.Exec(ctx, "create_user", "john.doe", "John Doe")
func (c *Connector) Exec(ctx context.Context, name string, args ...interface{}) error {
	return c.loader.exec(ctx, name, args...)
}

// QueryRow executes a named SQL query that returns a single row.
//
// Parameters:
//   - ctx: The context for the query execution
//   - name: The name of the query to execute
//   - scanner: A function that scans the result row
//   - args: Arguments for the query placeholders
//
// Example:
//
//	var id int
//	var username, name string
//
//	err := conn.QueryRow(ctx, "get_user_by_id", func(row pgx.Row) error {
//	    return row.Scan(&id, &username, &name)
//	}, 1)
func (c *Connector) QueryRow(ctx context.Context, name string, scanner func(pgx.Row) error, args ...interface{}) error {
	return c.loader.queryRow(ctx, name, scanner, args...)
}

// QueryRows executes a named SQL query that returns multiple rows.
//
// Parameters:
//   - ctx: The context for the query execution
//   - name: The name of the query to execute
//   - scanner: A function that processes the result rows
//   - args: Arguments for the query placeholders
//
// Example:
//
//	err := conn.QueryRows(ctx, "list_users", func(rows pgx.Rows) error {
//	    for rows.Next() {
//	        var id int
//	        var username, name string
//	        if err := rows.Scan(&id, &username, &name); err != nil {
//	            return err
//	        }
//	        fmt.Printf("User: %d, %s, %s\n", id, username, name)
//	    }
//	    return nil
//	})
func (c *Connector) QueryRows(ctx context.Context, name string, scanner func(pgx.Rows) error, args ...interface{}) error {
	return c.loader.queryRows(ctx, name, scanner, args...)
}

// InitiateMigration initializes the migration manager and ensures the migrations table exists.
//
// This method is called automatically by Migrate and Rollback, but you can call it
// explicitly if you need to ensure the migrations table exists without applying migrations.
func (c *Connector) InitiateMigration(ctx context.Context) error {
	c.reader.migrations = newMigrationManager(c.db, c.reader.queriesFS, c.reader.migrationsDir)
	return c.reader.migrations.Initialize(ctx)
}

// Migrate applies all pending migrations.
//
// This method automatically starts a transaction if one isn't already in progress,
// applies all pending migrations, and commits the transaction if successful.
//
// Example:
//
//	// Apply all pending migrations
//	if err := conn.Migrate(ctx); err != nil {
//	    log.Fatal(err)
//	}
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

// Rollback reverts the last applied migration.
//
// This method automatically starts a transaction if one isn't already in progress,
// reverts the last migration, and commits the transaction if successful.
//
// Example:
//
//	// Rollback the last migration
//	if err := conn.Rollback(ctx); err != nil {
//	    log.Fatal(err)
//	}
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

// ExecuteJSONBQuery executes a query with JSONB support.
// This is a convenience method for working with JSONB columns.
//
// It works exactly like QueryRows but is named explicitly to indicate its purpose.
//
// Example:
//
//	err := conn.ExecuteJSONBQuery(ctx, "get_user_preferences", func(rows pgx.Rows) error {
//	    for rows.Next() {
//	        var id int
//	        var prefs map[string]interface{}
//	        if err := rows.Scan(&id, &prefs); err != nil {
//	            return err
//	        }
//	        fmt.Printf("User %d preferences: %v\n", id, prefs)
//	    }
//	    return nil
//	}, userId)
func (c *Connector) ExecuteJSONBQuery(ctx context.Context, name string, scanner func(pgx.Rows) error, args ...interface{}) error {
	return c.loader.queryRows(ctx, name, scanner, args...)
}

// JSONB Helper Functions
// These functions help construct PostgreSQL JSONB query expressions.

// JSONBContains generates a query fragment for JSONB containment (@>).
// This operator checks if the left JSONB value contains the right value.
//
// Example:
//
//	// Find users with dark theme preference
//	whereClause := sqlreader.JSONBContains("preferences", `{"theme": "dark"}`)
//	query := fmt.Sprintf("SELECT * FROM users WHERE %s", whereClause)
func JSONBContains(column string, value interface{}) string {
	return fmt.Sprintf("%s @> $1", column)
}

// JSONBContainedBy generates a query fragment for JSONB contained by (<@).
// This operator checks if the left JSONB value is contained by the right value.
//
// Example:
//
//	// Find preferences that are a subset of a template
//	whereClause := sqlreader.JSONBContainedBy("user_prefs", `{"theme": "dark", "notifications": true}`)
func JSONBContainedBy(column string, value interface{}) string {
	return fmt.Sprintf("%s <@ $1", column)
}

// JSONBHasKey generates a query fragment to check if a JSONB document has a key.
// This operator checks if the specified key exists at the top level of the JSONB value.
//
// Example:
//
//	// Find users that have set a theme preference
//	whereClause := sqlreader.JSONBHasKey("preferences", "theme")
func JSONBHasKey(column, key string) string {
	return fmt.Sprintf("%s ? %s", column, key)
}

// JSONBGetPath generates a query fragment to extract a value at a specified path.
// This operator extracts a JSON value from a JSONB document and returns it as text.
//
// Example:
//
//	// Extract the theme preference as text
//	selectClause := sqlreader.JSONBGetPath("preferences", "theme")
//	query := fmt.Sprintf("SELECT id, %s FROM users", selectClause)
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
