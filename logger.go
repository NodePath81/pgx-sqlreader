package sqlreader

import (
	"context"
	"log/slog"
	"os"
	"time"
)

// LogLevel represents the level of logging.
type LogLevel string

const (
	// LogLevelDebug sets logging to debug level.
	LogLevelDebug LogLevel = "debug"
	// LogLevelInfo sets logging to info level.
	LogLevelInfo LogLevel = "info"
	// LogLevelWarn sets logging to warn level.
	LogLevelWarn LogLevel = "warn"
	// LogLevelError sets logging to error level.
	LogLevelError LogLevel = "error"
)

// Logger is the interface used for logging within the sqlreader package.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	With(args ...any) Logger
}

// slogLogger is an implementation of Logger using slog.
type slogLogger struct {
	logger *slog.Logger
}

// Debug logs a debug message.
func (l *slogLogger) Debug(msg string, args ...any) {
	l.logger.Debug(msg, args...)
}

// Info logs an info message.
func (l *slogLogger) Info(msg string, args ...any) {
	l.logger.Info(msg, args...)
}

// Warn logs a warning message.
func (l *slogLogger) Warn(msg string, args ...any) {
	l.logger.Warn(msg, args...)
}

// Error logs an error message.
func (l *slogLogger) Error(msg string, args ...any) {
	l.logger.Error(msg, args...)
}

// With returns a new Logger with the given attributes added to each log entry.
func (l *slogLogger) With(args ...any) Logger {
	return &slogLogger{
		logger: l.logger.With(args...),
	}
}

// NewLogger creates a new Logger with the given level.
func NewLogger(level LogLevel) Logger {
	var logLevel slog.Level
	switch level {
	case LogLevelDebug:
		logLevel = slog.LevelDebug
	case LogLevelInfo:
		logLevel = slog.LevelInfo
	case LogLevelWarn:
		logLevel = slog.LevelWarn
	case LogLevelError:
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})

	return &slogLogger{
		logger: slog.New(handler),
	}
}

// defaultLogger is the default logger used if no logger is provided.
var defaultLogger = NewLogger(LogLevelInfo)

// contextKey is a type for context keys.
type contextKey string

// loggerKey is the context key for the logger.
const loggerKey contextKey = "sqlreader-logger"

// ContextWithLogger adds a logger to a context.
func ContextWithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// LoggerFromContext gets a logger from a context.
// If no logger is found, it returns the default logger.
func LoggerFromContext(ctx context.Context) Logger {
	if logger, ok := ctx.Value(loggerKey).(Logger); ok {
		return logger
	}
	return defaultLogger
}

// WithOperation adds operation information to a logger.
func WithOperation(logger Logger, operation string) Logger {
	return logger.With("operation", operation)
}

// WithDuration adds execution duration to a logger.
func WithDuration(logger Logger, duration time.Duration) Logger {
	return logger.With("duration_ms", duration.Milliseconds())
}
