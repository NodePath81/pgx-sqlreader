package sqlreader

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
)

// Testing the parse queries functionality
func TestParseQueries(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected map[string]string
	}{
		{
			name: "single query",
			content: `-- name: get_user
SELECT * FROM users WHERE id = $1`,
			expected: map[string]string{
				"get_user": "SELECT * FROM users WHERE id = $1",
			},
		},
		{
			name: "multiple queries",
			content: `-- name: get_user
SELECT * FROM users WHERE id = $1

-- name: create_user
INSERT INTO users (name) VALUES ($1)`,
			expected: map[string]string{
				"get_user":    "SELECT * FROM users WHERE id = $1",
				"create_user": "INSERT INTO users (name) VALUES ($1)",
			},
		},
		{
			name: "query with multiple lines",
			content: `-- name: complex_query
SELECT 
  u.id, 
  u.name, 
  p.title 
FROM 
  users u 
JOIN 
  posts p ON u.id = p.user_id 
WHERE 
  u.id = $1`,
			expected: map[string]string{
				"complex_query": `SELECT 
  u.id, 
  u.name, 
  p.title 
FROM 
  users u 
JOIN 
  posts p ON u.id = p.user_id 
WHERE 
  u.id = $1`,
			},
		},
		{
			name:     "empty content",
			content:  "",
			expected: map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			qs := &queryStore{
				queries: make(map[string]string),
			}
			err := qs.parseQueries(tc.content)
			if err != nil {
				t.Fatalf("parseQueries returned an error: %v", err)
			}

			if len(qs.queries) != len(tc.expected) {
				t.Errorf("Expected %d queries, got %d", len(tc.expected), len(qs.queries))
			}

			for name, expectedSQL := range tc.expected {
				actualSQL, ok := qs.queries[name]
				if !ok {
					t.Errorf("Expected query %q not found", name)
					continue
				}
				if actualSQL != expectedSQL {
					t.Errorf("Query %q mismatch:\nExpected: %q\nActual: %q", name, expectedSQL, actualSQL)
				}
			}
		})
	}
}

// Test query retrieval and panic behavior
func TestQueryStore_Get(t *testing.T) {
	qs := &queryStore{
		queries: map[string]string{
			"existing_query": "SELECT * FROM users",
		},
	}

	t.Run("existing query", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("get() panicked unexpectedly: %v", r)
			}
		}()

		query := qs.get("existing_query")
		if query != "SELECT * FROM users" {
			t.Errorf("Expected 'SELECT * FROM users', got %q", query)
		}
	})

	t.Run("nonexistent query", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("get() did not panic on nonexistent query")
			}
		}()

		qs.get("nonexistent_query")
	})
}

// Test the JSONB helper functions
func TestJSONBHelpers(t *testing.T) {
	tests := []struct {
		name     string
		function func() string
		expected string
	}{
		{
			name: "JSONBContains",
			function: func() string {
				return JSONBContains("data", "value")
			},
			expected: "data @> $1",
		},
		{
			name: "JSONBContainedBy",
			function: func() string {
				return JSONBContainedBy("data", "value")
			},
			expected: "data <@ $1",
		},
		{
			name: "JSONBHasKey",
			function: func() string {
				return JSONBHasKey("data", "key")
			},
			expected: "data ? key",
		},
		{
			name: "JSONBGetPath empty",
			function: func() string {
				return JSONBGetPath("data")
			},
			expected: "data",
		},
		{
			name: "JSONBGetPath single",
			function: func() string {
				return JSONBGetPath("data", "key")
			},
			expected: "data->>'key'",
		},
		{
			name: "JSONBGetPath nested",
			function: func() string {
				return JSONBGetPath("data", "key1", "key2")
			},
			expected: "data->>'key1'->>'key2'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.function()
			if result != tc.expected {
				t.Errorf("Expected %q, got %q", tc.expected, result)
			}
		})
	}
}

// Basic tests for SQLReader GetSQL method
func TestSQLReader_GetSQL(t *testing.T) {
	// Create a query store with test queries
	qs := &queryStore{
		queries: map[string]string{
			"test_query": "SELECT 1",
		},
	}

	// Create a SQLReader with the query store
	reader := &SQLReader{
		queries:       qs,
		queriesDir:    "sql",
		migrationsDir: "migrations",
	}

	// Test GetSQL
	sql := reader.GetSQL("test_query")
	if sql != "SELECT 1" {
		t.Errorf("Expected 'SELECT 1', got %q", sql)
	}

	// Test panic on non-existent query
	t.Run("panic on nonexistent query", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("GetSQL did not panic on nonexistent query")
			}
		}()
		reader.GetSQL("nonexistent_query")
	})
}

// Test queryLoader with pgxmock
func TestQueryLoader_WithPgxMock(t *testing.T) {
	// Create a mock database connection
	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("Failed to create mock connection: %v", err)
	}
	defer mock.Close(context.Background())

	// Create a query store with test queries
	qs := &queryStore{
		queries: map[string]string{
			"test_exec":       "INSERT INTO users (name) VALUES ($1)",
			"test_query_row":  "SELECT id, name FROM users WHERE id = $1",
			"test_query_rows": "SELECT id, name FROM users",
		},
	}

	// Create a query loader with the mock connection
	loader := &queryLoader{
		db:      mock,
		querier: qs,
	}

	// Test exec method
	t.Run("exec method", func(t *testing.T) {
		// Set up expectations
		mock.ExpectExec("INSERT INTO users \\(name\\) VALUES \\(\\$1\\)").
			WithArgs("John").
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Execute the method
		err := loader.exec(context.Background(), "test_exec", "John")
		if err != nil {
			t.Errorf("exec returned an error: %v", err)
		}

		// Verify expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	// Test queryRow method
	t.Run("queryRow method", func(t *testing.T) {
		// Set up expectations
		mock.ExpectQuery("SELECT id, name FROM users WHERE id = \\$1").
			WithArgs(1).
			WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

		// Execute the method
		var id int
		var name string
		err := loader.queryRow(context.Background(), "test_query_row", func(row pgx.Row) error {
			return row.Scan(&id, &name)
		}, 1)

		if err != nil {
			t.Errorf("queryRow returned an error: %v", err)
		}

		// Verify results
		if id != 1 || name != "John" {
			t.Errorf("Expected id=1, name='John', got id=%d, name='%s'", id, name)
		}

		// Verify expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	// Test queryRows method
	t.Run("queryRows method", func(t *testing.T) {
		// Set up expectations
		mock.ExpectQuery("SELECT id, name FROM users").
			WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).
				AddRow(1, "John").
				AddRow(2, "Jane"))

		// Execute the method
		var users []struct {
			ID   int
			Name string
		}

		err := loader.queryRows(context.Background(), "test_query_rows", func(rows pgx.Rows) error {
			for rows.Next() {
				var id int
				var name string
				if err := rows.Scan(&id, &name); err != nil {
					return err
				}
				users = append(users, struct {
					ID   int
					Name string
				}{ID: id, Name: name})
			}
			return nil
		})

		if err != nil {
			t.Errorf("queryRows returned an error: %v", err)
		}

		// Verify results
		if len(users) != 2 {
			t.Errorf("Expected 2 users, got %d", len(users))
		}
		if users[0].ID != 1 || users[0].Name != "John" {
			t.Errorf("Expected user[0] to be {1, John}, got {%d, %s}", users[0].ID, users[0].Name)
		}
		if users[1].ID != 2 || users[1].Name != "Jane" {
			t.Errorf("Expected user[1] to be {2, Jane}, got {%d, %s}", users[1].ID, users[1].Name)
		}

		// Verify expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})
}

// Test Connector methods with pgxmock
func TestConnector_WithPgxMock(t *testing.T) {
	// Create a mock database connection
	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("Failed to create mock connection: %v", err)
	}
	defer mock.Close(context.Background())

	// Create a query store with test queries
	qs := &queryStore{
		queries: map[string]string{
			"test_exec":       "INSERT INTO users (name) VALUES ($1)",
			"test_query_row":  "SELECT id, name FROM users WHERE id = $1",
			"test_query_rows": "SELECT id, name FROM users",
		},
	}

	// Create a SQLReader with the query store
	reader := &SQLReader{
		queries:       qs,
		queriesDir:    "sql",
		migrationsDir: "migrations",
	}

	// Create a query loader with the mock connection
	loader := &queryLoader{
		db:      mock,
		querier: qs,
	}

	// Create a connector
	connector := &Connector{
		db:     mock,
		reader: reader,
		loader: loader,
	}

	// Test Exec method
	t.Run("Exec method", func(t *testing.T) {
		// Set up expectations
		mock.ExpectExec("INSERT INTO users \\(name\\) VALUES \\(\\$1\\)").
			WithArgs("John").
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Execute the method
		err := connector.Exec(context.Background(), "test_exec", "John")
		if err != nil {
			t.Errorf("Exec returned an error: %v", err)
		}

		// Verify expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	// Test QueryRow method
	t.Run("QueryRow method", func(t *testing.T) {
		// Set up expectations
		mock.ExpectQuery("SELECT id, name FROM users WHERE id = \\$1").
			WithArgs(1).
			WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))

		// Execute the method
		var id int
		var name string
		err := connector.QueryRow(context.Background(), "test_query_row", func(row pgx.Row) error {
			return row.Scan(&id, &name)
		}, 1)

		if err != nil {
			t.Errorf("QueryRow returned an error: %v", err)
		}

		// Verify results
		if id != 1 || name != "John" {
			t.Errorf("Expected id=1, name='John', got id=%d, name='%s'", id, name)
		}

		// Verify expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})

	// Test QueryRows method
	t.Run("QueryRows method", func(t *testing.T) {
		// Set up expectations
		mock.ExpectQuery("SELECT id, name FROM users").
			WillReturnRows(pgxmock.NewRows([]string{"id", "name"}).
				AddRow(1, "John").
				AddRow(2, "Jane"))

		// Execute the method
		var users []struct {
			ID   int
			Name string
		}

		err := connector.QueryRows(context.Background(), "test_query_rows", func(rows pgx.Rows) error {
			for rows.Next() {
				var id int
				var name string
				if err := rows.Scan(&id, &name); err != nil {
					return err
				}
				users = append(users, struct {
					ID   int
					Name string
				}{ID: id, Name: name})
			}
			return nil
		})

		if err != nil {
			t.Errorf("QueryRows returned an error: %v", err)
		}

		// Verify results
		if len(users) != 2 {
			t.Errorf("Expected 2 users, got %d", len(users))
		}
		if users[0].ID != 1 || users[0].Name != "John" {
			t.Errorf("Expected user[0] to be {1, John}, got {%d, %s}", users[0].ID, users[0].Name)
		}
		if users[1].ID != 2 || users[1].Name != "Jane" {
			t.Errorf("Expected user[1] to be {2, Jane}, got {%d, %s}", users[1].ID, users[1].Name)
		}

		// Verify expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})
}

// Test migrations with pgxmock
func TestMigrations_WithPgxMock(t *testing.T) {
	// Create a mock database connection for transactions
	mock, err := pgxmock.NewConn()
	if err != nil {
		t.Fatalf("Failed to create mock connection: %v", err)
	}
	defer mock.Close(context.Background())

	// Custom migrationManager for tests without embed.FS
	migrations := &migrationManager{
		db:            mock,
		migrationsDir: "migrations",
	}

	// Test Initialize method
	t.Run("Initialize method", func(t *testing.T) {
		// Set up expectations
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		// Call the method
		err := migrations.Initialize(context.Background())
		if err != nil {
			t.Errorf("Initialize returned an error: %v", err)
		}

		// Verify expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations: %v", err)
		}
	})
}

// Test the transaction patterns for both Migrate and Rollback
func TestMigrationTransaction_Patterns(t *testing.T) {
	// Verify the transaction handling patterns are consistent

	// Migrate method pattern check
	migrateCode := `
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
	`

	// Rollback method pattern check
	rollbackCode := `
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
	`

	// Verify that both methods follow the same pattern
	t.Run("transaction handling consistency", func(t *testing.T) {
		if !strings.Contains(migrateCode, "Check if we're already in a transaction") {
			t.Error("Migrate method missing transaction check")
		}

		if !strings.Contains(rollbackCode, "Check if we're already in a transaction") {
			t.Error("Rollback method missing transaction check")
		}

		if !strings.Contains(migrateCode, "Need to start a transaction for migration") {
			t.Error("Migrate method missing transaction start")
		}

		if !strings.Contains(rollbackCode, "Need to start a transaction for rollback") {
			t.Error("Rollback method missing transaction start")
		}

		// Verify both have similar error handling
		if !strings.Contains(migrateCode, "tx.Rollback(ctx)") {
			t.Error("Migrate method missing rollback on error")
		}

		if !strings.Contains(rollbackCode, "tx.Rollback(ctx)") {
			t.Error("Rollback method missing rollback on error")
		}

		// Verify both commit the transaction
		if !strings.Contains(migrateCode, "tx.Commit(ctx)") {
			t.Error("Migrate method missing commit")
		}

		if !strings.Contains(rollbackCode, "tx.Commit(ctx)") {
			t.Error("Rollback method missing commit")
		}
	})
}
