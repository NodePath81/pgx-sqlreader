package sqlreader

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsConfig contains configuration for metrics.
type MetricsConfig struct {
	// Enabled determines if metrics collection is enabled.
	Enabled bool
	// Namespace is the prometheus namespace for metrics.
	Namespace string
	// Subsystem is the prometheus subsystem for metrics.
	Subsystem string
	// HandlerPath is the HTTP path to expose metrics.
	HandlerPath string
}

// DefaultMetricsConfig provides default configuration for metrics.
var DefaultMetricsConfig = MetricsConfig{
	Enabled:     true,
	Namespace:   "sqlreader",
	Subsystem:   "database",
	HandlerPath: "/metrics",
}

// MetricsCollector collects metrics for the sqlreader package.
type MetricsCollector interface {
	// ObserveQueryExecution records the duration of a query execution.
	ObserveQueryExecution(queryName string, duration time.Duration, success bool)
	// ObserveMigration records the duration of a migration.
	ObserveMigration(version int, name string, duration time.Duration, success bool)
	// IncrementError increments the error counter for a specific operation.
	IncrementError(operation string)
	// RegisterHTTPHandler registers the metrics HTTP handler.
	RegisterHTTPHandler(mux *http.ServeMux)
}

// prometheusCollector is an implementation of MetricsCollector using Prometheus.
type prometheusCollector struct {
	enabled           bool
	queryDuration     *prometheus.HistogramVec
	migrationDuration *prometheus.HistogramVec
	errorCounter      *prometheus.CounterVec
	handlerPath       string
}

// NewMetricsCollector creates a new MetricsCollector.
func NewMetricsCollector(config MetricsConfig) MetricsCollector {
	if !config.Enabled {
		return &noopCollector{}
	}

	queryDuration := promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "query_duration_seconds",
			Help:      "Duration of SQL query executions in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"query_name", "success"},
	)

	migrationDuration := promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "migration_duration_seconds",
			Help:      "Duration of database migrations in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"version", "name", "success"},
	)

	errorCounter := promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: config.Namespace,
			Subsystem: config.Subsystem,
			Name:      "errors_total",
			Help:      "Total number of errors in database operations",
		},
		[]string{"operation"},
	)

	return &prometheusCollector{
		enabled:           true,
		queryDuration:     queryDuration,
		migrationDuration: migrationDuration,
		errorCounter:      errorCounter,
		handlerPath:       config.HandlerPath,
	}
}

// ObserveQueryExecution records the duration of a query execution.
func (c *prometheusCollector) ObserveQueryExecution(queryName string, duration time.Duration, success bool) {
	if !c.enabled {
		return
	}
	successStr := "false"
	if success {
		successStr = "true"
	}
	c.queryDuration.WithLabelValues(queryName, successStr).Observe(duration.Seconds())
}

// ObserveMigration records the duration of a migration.
func (c *prometheusCollector) ObserveMigration(version int, name string, duration time.Duration, success bool) {
	if !c.enabled {
		return
	}
	versionStr := prometheus.BuildFQName("", "", strconv.Itoa(version))
	successStr := "false"
	if success {
		successStr = "true"
	}
	c.migrationDuration.WithLabelValues(versionStr, name, successStr).Observe(duration.Seconds())
}

// IncrementError increments the error counter for a specific operation.
func (c *prometheusCollector) IncrementError(operation string) {
	if !c.enabled {
		return
	}
	c.errorCounter.WithLabelValues(operation).Inc()
}

// RegisterHTTPHandler registers the metrics HTTP handler.
func (c *prometheusCollector) RegisterHTTPHandler(mux *http.ServeMux) {
	if !c.enabled {
		return
	}
	mux.Handle(c.handlerPath, promhttp.Handler())
}

// noopCollector is a no-op implementation of MetricsCollector.
type noopCollector struct{}

func (c *noopCollector) ObserveQueryExecution(queryName string, duration time.Duration, success bool) {
}
func (c *noopCollector) ObserveMigration(version int, name string, duration time.Duration, success bool) {
}
func (c *noopCollector) IncrementError(operation string)        {}
func (c *noopCollector) RegisterHTTPHandler(mux *http.ServeMux) {}

// defaultMetricsCollector is the default metrics collector.
var defaultMetricsCollector = NewMetricsCollector(DefaultMetricsConfig)

// metricsKey is the context key for the metrics collector.
const metricsKey contextKey = "sqlreader-metrics"

// ContextWithMetrics adds a metrics collector to a context.
func ContextWithMetrics(ctx context.Context, collector MetricsCollector) context.Context {
	return context.WithValue(ctx, metricsKey, collector)
}

// MetricsFromContext gets a metrics collector from a context.
// If no collector is found, it returns the default collector.
func MetricsFromContext(ctx context.Context) MetricsCollector {
	if collector, ok := ctx.Value(metricsKey).(MetricsCollector); ok {
		return collector
	}
	return defaultMetricsCollector
}

// GetMetricsHandler returns a http.Handler for exposing Prometheus metrics.
// This function provides a consistent way to access the metrics HTTP handler
// without creating duplicate metric registrations.
func GetMetricsHandler() http.Handler {
	return promhttp.Handler()
}

// TrackDuration tracks the duration of a function and records it as a metric.
// Returns a function that should be deferred to end tracking.
func TrackDuration(ctx context.Context, operation string) (context.Context, func(success bool)) {
	logger := LoggerFromContext(ctx)
	metrics := MetricsFromContext(ctx)
	startTime := time.Now()

	logger = WithOperation(logger, operation)
	ctx = ContextWithLogger(ctx, logger)

	return ctx, func(success bool) {
		duration := time.Since(startTime)
		WithDuration(logger, duration).Debug("Operation completed",
			"success", success,
			"operation", operation)

		if operation == "query" || operation == "exec" || operation == "queryRow" || operation == "queryRows" {
			metrics.ObserveQueryExecution(operation, duration, success)
		} else if operation == "migrate" || operation == "rollback" {
			// When used with migrations, additional information should be provided separately
			// through the ObserveMigration method
		}

		if !success {
			metrics.IncrementError(operation)
		}
	}
}
