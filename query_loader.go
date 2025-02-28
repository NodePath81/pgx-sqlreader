package sqlreader

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// queryLoader wraps a database connection and query store to provide
// convenient methods for executing queries
type queryLoader struct {
	db      dbConn
	querier *queryStore
	logger  Logger
	metrics MetricsCollector
}

// dbConn is an interface that abstracts the database connection.
// It can be either a *pgxpool.Pool or pgx.Tx, allowing the queryLoader
// to work with both connection pools and transactions.
type dbConn interface {
	// Exec executes a SQL query that doesn't return rows
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)

	// Query executes a SQL query that returns rows
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)

	// QueryRow executes a SQL query that returns at most one row
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// exec loads and executes a query that doesn't return any rows.
// It gets the SQL query by name from the query store and executes it with the provided arguments.
func (l *queryLoader) exec(ctx context.Context, name string, args ...interface{}) error {
	query := l.querier.get(name)

	logger := LoggerFromContext(ctx)
	logger = logger.With("query_name", name, "query_type", "exec")

	startTime := time.Now()

	logger.Debug("Executing query", "sql", query)

	_, err := l.db.Exec(ctx, query, args...)
	duration := time.Since(startTime)

	if err != nil {
		logger.Error("Query execution failed",
			"error", err,
			"duration_ms", duration.Milliseconds())

		metrics := MetricsFromContext(ctx)
		metrics.ObserveQueryExecution(name, duration, false)
		metrics.IncrementError("query_execution")

		return fmt.Errorf("executing %s: %w", name, err)
	}

	logger.Debug("Query executed successfully",
		"duration_ms", duration.Milliseconds())

	metrics := MetricsFromContext(ctx)
	metrics.ObserveQueryExecution(name, duration, true)

	return nil
}

// queryRow loads and executes a query that returns a single row.
// It gets the SQL query by name from the query store, executes it with the provided arguments,
// and passes the result row to the scanner function.
func (l *queryLoader) queryRow(ctx context.Context, name string, scanner func(pgx.Row) error, args ...interface{}) error {
	query := l.querier.get(name)

	logger := LoggerFromContext(ctx)
	logger = logger.With("query_name", name, "query_type", "queryRow")

	startTime := time.Now()

	logger.Debug("Executing query", "sql", query)

	row := l.db.QueryRow(ctx, query, args...)

	if err := scanner(row); err != nil {
		duration := time.Since(startTime)

		logger.Error("Query result scan failed",
			"error", err,
			"duration_ms", duration.Milliseconds())

		metrics := MetricsFromContext(ctx)
		metrics.ObserveQueryExecution(name, duration, false)
		metrics.IncrementError("query_scan")

		return fmt.Errorf("scanning %s result: %w", name, err)
	}

	duration := time.Since(startTime)
	logger.Debug("Query executed and scanned successfully",
		"duration_ms", duration.Milliseconds())

	metrics := MetricsFromContext(ctx)
	metrics.ObserveQueryExecution(name, duration, true)

	return nil
}

// queryRows loads and executes a query that returns multiple rows.
// It gets the SQL query by name from the query store, executes it with the provided arguments,
// and passes the result rows to the scanner function.
func (l *queryLoader) queryRows(ctx context.Context, name string, scanner func(pgx.Rows) error, args ...interface{}) error {
	query := l.querier.get(name)

	logger := LoggerFromContext(ctx)
	logger = logger.With("query_name", name, "query_type", "queryRows")

	startTime := time.Now()

	logger.Debug("Executing query", "sql", query)

	rows, err := l.db.Query(ctx, query, args...)
	if err != nil {
		duration := time.Since(startTime)

		logger.Error("Query execution failed",
			"error", err,
			"duration_ms", duration.Milliseconds())

		metrics := MetricsFromContext(ctx)
		metrics.ObserveQueryExecution(name, duration, false)
		metrics.IncrementError("query_execution")

		return fmt.Errorf("executing %s query: %w", name, err)
	}
	defer rows.Close()

	if err := scanner(rows); err != nil {
		duration := time.Since(startTime)

		logger.Error("Query result scan failed",
			"error", err,
			"duration_ms", duration.Milliseconds())

		metrics := MetricsFromContext(ctx)
		metrics.ObserveQueryExecution(name, duration, false)
		metrics.IncrementError("query_scan")

		return fmt.Errorf("scanning %s results: %w", name, err)
	}

	duration := time.Since(startTime)
	logger.Debug("Query executed and scanned successfully",
		"duration_ms", duration.Milliseconds())

	metrics := MetricsFromContext(ctx)
	metrics.ObserveQueryExecution(name, duration, true)

	return rows.Err()
}
