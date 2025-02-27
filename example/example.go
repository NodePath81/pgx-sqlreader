package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"os"

	sqlreader "github.com/NodePath81/pgx-sqlreader"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/*.sql
//go:embed migrations/*.sql
var embeddedFiles embed.FS

func main() {
	// Initialize SQL reader with embedded SQL files
	reader, err := sqlreader.New(embeddedFiles, "sql", "migrations")
	if err != nil {
		log.Fatalf("Failed to initialize SQL reader: %v", err)
	}

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

	// Create a connector with the database pool
	conn := reader.ConnectPool(pool)

	// Ensure migrations are applied
	if err := conn.Migrate(context.Background()); err != nil {
		log.Fatalf("Failed to apply migrations: %v", err)
	}

	// Example: Execute a query with a specific name
	err = conn.Exec(context.Background(), "create_user", "john.doe", "John Doe")
	if err != nil {
		log.Fatalf("Failed to execute create_user query: %v", err)
	}

	// Example: Query a single row
	var id int
	var username, name string
	err = conn.QueryRow(
		context.Background(),
		"get_user_by_username",
		func(row pgx.Row) error {
			return row.Scan(&id, &username, &name)
		},
		"john.doe",
	)
	if err != nil {
		log.Fatalf("Failed to execute get_user_by_username query: %v", err)
	}
	fmt.Printf("User: ID=%d, Username=%s, Name=%s\n", id, username, name)

	// Example: Query multiple rows
	err = conn.QueryRows(
		context.Background(),
		"list_users",
		func(rows pgx.Rows) error {
			for rows.Next() {
				var id int
				var username, name string
				if err := rows.Scan(&id, &username, &name); err != nil {
					return err
				}
				fmt.Printf("User: ID=%d, Username=%s, Name=%s\n", id, username, name)
			}
			return nil
		},
	)
	if err != nil {
		log.Fatalf("Failed to execute list_users query: %v", err)
	}

	// Example: Working with JSONB data
	jsonData := `{"preferences": {"theme": "dark", "notifications": true}}`
	err = conn.Exec(context.Background(), "update_user_preferences", jsonData, "john.doe")
	if err != nil {
		log.Fatalf("Failed to update user preferences: %v", err)
	}

	fmt.Println("Example completed successfully!")
}
