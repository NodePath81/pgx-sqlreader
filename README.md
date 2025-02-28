# pgx-sqlreader

A PostgreSQL SQL file reader and migration manager for Go applications that use the pgx driver.

## Features

- **Compile-time SQL loading**: Embeds SQL files into your Go binary during compilation
- **Named SQL queries**: Organize and access SQL queries by name
- **Migration management**: Handle database migrations with up and down migrations
- **Flexible schema evolution**: Support for phased migrations and incremental schema changes
- **JSONB support**: Helper functions for working with PostgreSQL's JSONB data type
- **Simple API**: Easy-to-use API for executing queries and managing migrations
- **pgx integration**: Works with pgx/v5 pools and transactions

## Installation

```bash
go get github.com/NodePath81/pgx-sqlreader
```

## Usage

### Directory Structure

Organize your SQL files and migrations in separate directories:

```
└── your-project/
    ├── sql/
    │   ├── users.sql
    │   └── products.sql
    ├── migrations/
    │   ├── 001_create_users.sql
    │   ├── 002_create_products.sql
    │   └── 003_add_user_preferences.sql
    └── main.go
```

### SQL Query Format

Structure your SQL files with named queries:

```sql
-- name: get_user_by_id
SELECT id, username, name
FROM users
WHERE id = $1

-- name: create_user
INSERT INTO users (username, name)
VALUES ($1, $2)
RETURNING id
```

### Migration Format

Structure your migration files with up and down sections:

```sql
-- Create a users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Down
DROP TABLE IF EXISTS users;
```

### Basic Usage

```go
package main

import (
    "context"
    "embed"
    "log"
    
    "github.com/NodePath81/pgx-sqlreader"
    "github.com/jackc/pgx/v5/pgxpool"
)

//go:embed sql/*.sql
//go:embed migrations/*.sql
var embeddedFiles embed.FS

func main() {
    // Initialize SQL reader
    reader, err := sqlreader.New(embeddedFiles, "sql", "migrations")
    if err != nil {
        log.Fatalf("Failed to initialize SQL reader: %v", err)
    }
    
    // Connect to database
    pool, err := pgxpool.New(context.Background(), "postgres://postgres:password@localhost:5432/mydb")
    if err != nil {
        log.Fatalf("Unable to connect to database: %v", err)
    }
    defer pool.Close()
    
    // Create a connector
    conn := reader.ConnectPool(pool)
    
    // Apply migrations
    if err := conn.Migrate(context.Background()); err != nil {
        log.Fatalf("Failed to apply migrations: %v", err)
    }
    
    // Execute a query
    if err := conn.Exec(context.Background(), "create_user", "john", "John Doe"); err != nil {
        log.Fatalf("Failed to create user: %v", err)
    }
}
```

### Advanced Migration: Two-Phase Approach

The library supports a phased migration approach, allowing you to evolve your database schema incrementally:

```go
// Phase 1: Apply only initial schema (e.g., just the users table)
tx, err := pool.Begin(context.Background())
if err != nil {
    log.Fatalf("Failed to begin transaction: %v", err)
}

// Create a connector with the transaction
txConn := reader.ConnectTx(tx)

// Initialize migrations table
if err := txConn.InitiateMigration(context.Background()); err != nil {
    tx.Rollback(context.Background())
    log.Fatalf("Failed to initialize migrations table: %v", err)
}

// Manually execute and record specific migrations
// ... apply your SQL here ...
// ... record migration in schema_migrations table ...

tx.Commit(context.Background())

// Work with the initial schema
// ...

// Phase 2: Apply remaining migrations
if err := conn.Migrate(context.Background()); err != nil {
    log.Fatalf("Failed to apply remaining migrations: %v", err)
}

// Work with the evolved schema
// ...
```

This approach is useful for:
- Deploying partial features while others are still in development
- Migrating large databases with minimal downtime
- Testing schema changes in production-like environments

See the complete example in `example/example.go` for a full demonstration.

### Working with Query Results

```go
// Query a single row
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

// Query multiple rows
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
            fmt.Printf("User: %d, %s, %s\n", id, username, name)
        }
        return nil
    },
)
```

### JSONB Support

```go
// Store JSONB data
jsonData := `{"preferences": {"theme": "dark", "notifications": true}}`
err = conn.Exec(
    context.Background(),
    "update_user_preferences",
    jsonData,
    "john.doe",
)

// Using JSONB helpers for custom queries
query := fmt.Sprintf(`
    SELECT id, username, name
    FROM users 
    WHERE %s
`, sqlreader.JSONBContains("preferences", `{"theme": "dark"}`))

// Query users with specific preferences
```

## License

MIT
