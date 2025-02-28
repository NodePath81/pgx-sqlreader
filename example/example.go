package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	sqlreader "github.com/NodePath81/pgx-sqlreader"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/*.sql
//go:embed migrations/*.sql
var embeddedFiles embed.FS

// This example demonstrates how to use the sqlreader package to
// manage SQL queries and migrations in a PostgreSQL database.
// It shows a two-phase migration approach, first creating a basic schema
// and then applying additional migrations to evolve the schema.
func main() {
	fmt.Println("SQLReader Example - Migration Demonstration")
	fmt.Println("===========================================")

	// Configure custom logging and metrics
	// Set up a more verbose logger for development
	config := sqlreader.DefaultSQLReaderConfig
	config.LogLevel = sqlreader.LogLevelDebug

	// Configure metrics with a custom prefix
	config.MetricsConfig.Namespace = "example"
	config.MetricsConfig.Subsystem = "sqlreader"

	// Connect to database
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		connString = "postgres://postgres:postgres@localhost:5432/example"
	}

	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()

	// Reset the database for demonstration purposes
	resetDatabase(pool)

	// PART 1: INITIAL SCHEMA SETUP
	fmt.Println("\n📝 PHASE 1: Creating initial schema (users table only)")
	fmt.Println("-----------------------------------------------")

	// Initialize SQL reader with embedded SQL files and custom config
	reader, err := sqlreader.NewWithConfig(embeddedFiles, "sql", "migrations", config)
	if err != nil {
		log.Fatalf("Failed to initialize SQL reader: %v", err)
	}

	// Create a connector with the database pool
	conn := reader.ConnectPool(pool)

	// Set up metrics HTTP handler (optional - for demonstration)
	go setupMetricsServer()

	// Apply only the first migration (users table)
	// We simulate this by programmatically applying just the first migration
	tx, err := pool.Begin(context.Background())
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Create a connector with the transaction
	txConn := reader.ConnectTx(tx)

	// Create a context with logger for this operation
	ctx := context.Background()
	logger := sqlreader.NewLogger(sqlreader.LogLevelDebug)
	ctx = sqlreader.ContextWithLogger(ctx, logger.With("phase", "initial_schema"))

	// Initialize migrations table
	if err := txConn.InitiateMigration(ctx); err != nil {
		tx.Rollback(context.Background())
		log.Fatalf("Failed to initialize migrations table: %v", err)
	}

	// Manually execute just the first migration
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(100) NOT NULL UNIQUE,
			name VARCHAR(200) NOT NULL,
			preferences JSONB DEFAULT '{}'::jsonb,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		);
		
		CREATE INDEX IF NOT EXISTS idx_users_preferences ON users USING gin (preferences);
	`); err != nil {
		tx.Rollback(context.Background())
		log.Fatalf("Failed to create users table: %v", err)
	}

	// Record the migration
	if _, err := tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, name, applied_at)
		VALUES (1, 'create_users', $1)
	`, time.Now().UTC()); err != nil {
		tx.Rollback(context.Background())
		log.Fatalf("Failed to record migration: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	fmt.Println("✅ Initial schema created successfully (users table)")
	fmt.Println("✅ Migration recorded in schema_migrations table")

	// Work with the initial schema
	createAndQueryUsers(conn)

	// PART 2: SCHEMA EVOLUTION
	fmt.Println("\n📝 PHASE 2: Evolving schema (adding posts and comments tables)")
	fmt.Println("-----------------------------------------------------------")

	// Create a context with logger for this operation
	ctx = context.Background()
	logger = sqlreader.NewLogger(sqlreader.LogLevelDebug)
	ctx = sqlreader.ContextWithLogger(ctx, logger.With("phase", "schema_evolution"))

	// Apply remaining migrations
	fmt.Println("Applying remaining migrations...")
	if err := conn.Migrate(ctx); err != nil {
		log.Fatalf("Failed to apply remaining migrations: %v", err)
	}
	fmt.Println("✅ Schema evolution completed successfully")
	fmt.Println("✅ Added posts and comments tables")

	// Work with the evolved schema
	createPostsAndComments(conn)

	fmt.Println("\n🎉 Example completed successfully!")

	// Wait for a moment to allow metrics to be scraped if needed
	fmt.Println("\nMetrics server running at http://localhost:2112/metrics")
	fmt.Println("Press Ctrl+C to exit")

	select {}
}

// setupMetricsServer sets up an HTTP server to expose Prometheus metrics
func setupMetricsServer() {
	// Create a new HTTP mux
	mux := http.NewServeMux()

	// Get the metrics handler from the SQLReader instance
	// This avoids creating duplicate metrics collectors
	metrics := sqlreader.GetMetricsHandler()

	// Register the handler
	mux.Handle("/metrics", metrics)

	// Start HTTP server
	fmt.Println("Starting metrics server on :2112")
	if err := http.ListenAndServe(":2112", mux); err != nil {
		log.Printf("Metrics server stopped: %v", err)
	}
}

// createAndQueryUsers demonstrates creating and querying users with the initial schema
func createAndQueryUsers(conn *sqlreader.Connector) {
	// Create a context with logger for this operation
	ctx := context.Background()
	logger := sqlreader.NewLogger(sqlreader.LogLevelDebug)
	ctx = sqlreader.ContextWithLogger(ctx, logger.With("operation", "user_management"))

	// Create a user
	fmt.Println("\nCreating a user...")
	err := conn.Exec(ctx, "create_user", "john.doe", "John Doe")
	if err != nil {
		log.Fatalf("Failed to execute create_user query: %v", err)
	}

	// Query the user
	var id int
	var username, name string
	err = conn.QueryRow(
		ctx,
		"get_user_by_username",
		func(row pgx.Row) error {
			return row.Scan(&id, &username, &name)
		},
		"john.doe",
	)
	if err != nil {
		log.Fatalf("Failed to execute get_user_by_username query: %v", err)
	}
	fmt.Printf("User created: ID=%d, Username=%s, Name=%s\n", id, username, name)

	// Update user preferences
	jsonData := `{"preferences": {"theme": "dark", "notifications": true}}`
	err = conn.Exec(ctx, "update_user_preferences", jsonData, "john.doe")
	if err != nil {
		log.Fatalf("Failed to update user preferences: %v", err)
	}
	fmt.Println("User preferences updated")
}

// createPostsAndComments demonstrates creating and querying posts and comments
// with the evolved schema
func createPostsAndComments(conn *sqlreader.Connector) {
	// Create a context with logger for this operation
	ctx := context.Background()
	logger := sqlreader.NewLogger(sqlreader.LogLevelDebug)
	ctx = sqlreader.ContextWithLogger(ctx, logger.With("operation", "post_management"))

	var userId int

	// Get the user ID
	err := conn.QueryRow(
		ctx,
		"get_user_by_username",
		func(row pgx.Row) error {
			return row.Scan(&userId, nil, nil) // Only need the ID
		},
		"john.doe",
	)
	if err != nil {
		log.Fatalf("Failed to get user ID: %v", err)
	}

	// Create a post
	fmt.Println("\nCreating a post...")
	var postId int
	err = conn.QueryRow(
		ctx,
		"create_post",
		func(row pgx.Row) error {
			return row.Scan(&postId)
		},
		userId, "My First Post", "This is the content of my first post.",
	)
	if err != nil {
		log.Fatalf("Failed to create post: %v", err)
	}
	fmt.Printf("Post created with ID: %d\n", postId)

	// Create a comment
	fmt.Println("Adding a comment to the post...")
	var commentId int
	err = conn.QueryRow(
		ctx,
		"create_comment",
		func(row pgx.Row) error {
			return row.Scan(&commentId)
		},
		postId, userId, "This is a comment on my own post!",
	)
	if err != nil {
		log.Fatalf("Failed to create comment: %v", err)
	}
	fmt.Printf("Comment created with ID: %d\n", commentId)

	// Get post with comments count
	var count int
	err = conn.QueryRow(
		ctx,
		"count_post_comments",
		func(row pgx.Row) error {
			return row.Scan(&count)
		},
		postId,
	)
	if err != nil {
		log.Fatalf("Failed to count comments: %v", err)
	}
	fmt.Printf("Post has %d comments\n", count)

	// Demonstrate we can also get post with author information
	var title, content, username, authorName string
	var createdAt time.Time
	err = conn.QueryRow(
		ctx,
		"get_post_by_id",
		func(row pgx.Row) error {
			return row.Scan(&postId, &title, &content, &createdAt, &username, &authorName)
		},
		postId,
	)
	if err != nil {
		log.Fatalf("Failed to get post: %v", err)
	}
	fmt.Printf("Post details - Title: %s, Author: %s, Created: %s\n",
		title, authorName, createdAt.Format(time.RFC3339))
}

// resetDatabase drops all tables to ensure a clean demonstration
func resetDatabase(pool *pgxpool.Pool) {
	fmt.Println("Resetting database for demonstration...")

	// Drop tables in the correct order (respecting foreign keys)
	_, err := pool.Exec(context.Background(), `
		DROP TABLE IF EXISTS comments;
		DROP TABLE IF EXISTS posts;
		DROP TABLE IF EXISTS users;
		DROP TABLE IF EXISTS schema_migrations;
	`)
	if err != nil {
		log.Fatalf("Failed to reset database: %v", err)
	}
	fmt.Println("Database reset completed")
}
