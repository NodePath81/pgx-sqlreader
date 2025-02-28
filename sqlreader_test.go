package sqlreader

import (
	"context"
	"fmt"
	"testing"
	"time"

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

	// Test applying migrations multiple times (schema evolution)
	t.Run("Multiple migration applications", func(t *testing.T) {
		// Create a new mock connection for this test
		mock, err := pgxmock.NewConn()
		if err != nil {
			t.Fatalf("Failed to create mock connection: %v", err)
		}
		defer mock.Close(context.Background())

		// Set up expectations for first migration run
		// First expect schema_migrations table creation (this happens before transaction begins)
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		// Expect transaction to begin
		mock.ExpectBegin()

		// Expect query to get applied migrations (returns empty result since no migrations applied yet)
		mock.ExpectQuery("SELECT version, name, applied_at.*FROM schema_migrations.*").
			WillReturnRows(pgxmock.NewRows([]string{"version", "name", "applied_at"}))

		// Expect all 4 migrations to be applied in the first run
		// First migration: create_users
		mock.ExpectExec("CREATE TABLE users \\(id SERIAL PRIMARY KEY, name TEXT\\)").
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		// Expect recording of first migration
		mock.ExpectExec("INSERT INTO schema_migrations").
			WithArgs(1, "create_users", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Second migration: create_posts
		mock.ExpectExec("CREATE TABLE posts \\(id SERIAL PRIMARY KEY, title TEXT, user_id INTEGER REFERENCES users\\(id\\)\\)").
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		// Expect recording of second migration
		mock.ExpectExec("INSERT INTO schema_migrations").
			WithArgs(2, "create_posts", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Third migration: create_comments
		mock.ExpectExec("CREATE TABLE comments \\(id SERIAL PRIMARY KEY, content TEXT, post_id INTEGER REFERENCES posts\\(id\\)\\)").
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		// Expect recording of third migration
		mock.ExpectExec("INSERT INTO schema_migrations").
			WithArgs(3, "create_comments", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Fourth migration: add_email_to_users
		mock.ExpectExec("ALTER TABLE users ADD COLUMN email TEXT").
			WillReturnResult(pgxmock.NewResult("ALTER TABLE", 0))

		// Expect recording of fourth migration
		mock.ExpectExec("INSERT INTO schema_migrations").
			WithArgs(4, "add_email_to_users", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		// Expect transaction to commit
		mock.ExpectCommit()

		// Set up second migration run
		// Create a new mock to simulate a fresh connection
		mock2, err := pgxmock.NewConn()
		if err != nil {
			t.Fatalf("Failed to create second mock connection: %v", err)
		}
		defer mock2.Close(context.Background())

		// First expect schema_migrations table creation
		mock2.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		// Expect transaction to begin
		mock2.ExpectBegin()

		// This time return all previously applied migrations
		mock2.ExpectQuery("SELECT version, name, applied_at.*FROM schema_migrations.*").
			WillReturnRows(pgxmock.NewRows([]string{"version", "name", "applied_at"}).
				AddRow(1, "create_users", "2023-01-01T00:00:00Z").
				AddRow(2, "create_posts", "2023-01-01T00:00:00Z").
				AddRow(3, "create_comments", "2023-01-01T00:00:00Z").
				AddRow(4, "add_email_to_users", "2023-01-01T00:00:00Z"))

		// Expect transaction to commit as there's nothing to do
		mock2.ExpectCommit()

		// Set up third migration run with no changes expected
		// Create a new mock to simulate another fresh connection
		mock3, err := pgxmock.NewConn()
		if err != nil {
			t.Fatalf("Failed to create third mock connection: %v", err)
		}
		defer mock3.Close(context.Background())

		// First expect schema_migrations table creation
		mock3.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		// Expect transaction to begin
		mock3.ExpectBegin()

		// Return all previously applied migrations
		mock3.ExpectQuery("SELECT version, name, applied_at.*FROM schema_migrations.*").
			WillReturnRows(pgxmock.NewRows([]string{"version", "name", "applied_at"}).
				AddRow(1, "create_users", "2023-01-01T00:00:00Z").
				AddRow(2, "create_posts", "2023-01-01T00:00:00Z").
				AddRow(3, "create_comments", "2023-01-01T00:00:00Z").
				AddRow(4, "add_email_to_users", "2023-01-01T00:00:00Z"))

		// Expect transaction to commit as there's nothing to do
		mock3.ExpectCommit()

		// Now execute the migration sequence with fresh connections each time
		t.Run("First migration - initial schema", func(t *testing.T) {
			// Create a connector with the first mock
			connector := createConnectorForMigrationTest(mock, t)

			// Perform first migration
			err = connector.Migrate(context.Background())
			if err != nil {
				t.Errorf("First migration failed: %v", err)
			}
		})

		t.Run("Second migration - no changes", func(t *testing.T) {
			// Create a connector with the second mock
			connector := createConnectorForMigrationTest(mock2, t)

			// Perform second migration (no changes expected)
			err = connector.Migrate(context.Background())
			if err != nil {
				t.Errorf("Second migration failed: %v", err)
			}
		})

		t.Run("Third migration - still no changes", func(t *testing.T) {
			// Create a connector with the third mock
			connector := createConnectorForMigrationTest(mock3, t)

			// Perform third migration (no changes expected)
			err = connector.Migrate(context.Background())
			if err != nil {
				t.Errorf("Third migration failed: %v", err)
			}
		})

		// Verify expectations for each mock
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations for first migration: %v", err)
		}
		if err := mock2.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations for second migration: %v", err)
		}
		if err := mock3.ExpectationsWereMet(); err != nil {
			t.Errorf("Unfulfilled expectations for third migration: %v", err)
		}
	})
}

// Override connector for testing
type testConnector struct {
	*Connector
}

// Override the Migrate method to avoid pgxpool.Pool type assertion
func (c *testConnector) Migrate(ctx context.Context) error {
	// Initialize migration manager if needed
	if c.reader.migrations == nil {
		if err := c.InitiateMigration(ctx); err != nil {
			return err
		}
	}

	// For tests, always assume we need to start a new transaction
	// and skip the pgxpool.Pool type assertion
	tx, err := c.db.(pgxmock.PgxConnIface).Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting transaction for migration: %w", err)
	}

	// Create a new test migration manager with the transaction
	testMgr := &testMigrationManager{
		db:            tx,
		migrationsDir: c.reader.migrationsDir,
	}

	// Apply migrations
	if err := testMgr.Migrate(ctx); err != nil {
		tx.Rollback(ctx)
		return err
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing migration transaction: %w", err)
	}

	return nil
}

// Test-specific migration manager
type testMigrationManager struct {
	db            pgx.Tx
	migrationsDir string
}

func (m *testMigrationManager) Initialize(ctx context.Context) error {
	_, err := m.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  TIMESTAMP WITH TIME ZONE NOT NULL
		);
	`)
	return err
}

func (m *testMigrationManager) LoadMigrations() ([]migration, error) {
	// For tests, we return predefined migrations
	return []migration{
		{
			Version: 1,
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)",
		},
		{
			Version: 2,
			Name:    "create_posts",
			UpSQL:   "CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT, user_id INTEGER REFERENCES users(id))",
		},
		{
			Version: 3,
			Name:    "create_comments",
			UpSQL:   "CREATE TABLE comments (id SERIAL PRIMARY KEY, content TEXT, post_id INTEGER REFERENCES posts(id))",
		},
		{
			Version: 4,
			Name:    "add_email_to_users",
			UpSQL:   "ALTER TABLE users ADD COLUMN email TEXT",
		},
	}, nil
}

func (m *testMigrationManager) GetAppliedMigrations(ctx context.Context) (map[int]migration, error) {
	rows, err := m.db.Query(ctx, `
		SELECT version, name, applied_at
		FROM schema_migrations
		ORDER BY version ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]migration)
	for rows.Next() {
		var mig migration
		var appliedAtStr string
		err := rows.Scan(&mig.Version, &mig.Name, &appliedAtStr)
		if err != nil {
			return nil, err
		}
		applied[mig.Version] = mig
	}

	return applied, rows.Err()
}

func (m *testMigrationManager) Migrate(ctx context.Context) error {
	migrations, err := m.LoadMigrations()
	if err != nil {
		return err
	}

	applied, err := m.GetAppliedMigrations(ctx)
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		if _, exists := applied[migration.Version]; !exists {
			// Apply migration
			if _, err := m.db.Exec(ctx, migration.UpSQL); err != nil {
				return fmt.Errorf("applying migration %d: %w", migration.Version, err)
			}

			// Record migration
			if _, err := m.db.Exec(ctx, `
				INSERT INTO schema_migrations (version, name, applied_at)
				VALUES ($1, $2, $3)
			`, migration.Version, migration.Name, time.Now().UTC()); err != nil {
				return fmt.Errorf("recording migration %d: %w", migration.Version, err)
			}
		}
	}

	return nil
}

// Helper function to create a connector for migration testing
func createConnectorForMigrationTest(mock pgxmock.PgxConnIface, t *testing.T) *testConnector {
	// Create a query store
	qs := &queryStore{
		queries: map[string]string{},
	}

	// Create a SQLReader
	reader := &SQLReader{
		queries:       qs,
		queriesDir:    "sql",
		migrationsDir: "migrations",
	}

	// Create a connector
	connector := &Connector{
		db:     mock,
		reader: reader,
	}

	// Wrap in our test connector
	return &testConnector{
		Connector: connector,
	}
}

// Helper function to parse time strings
func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}
