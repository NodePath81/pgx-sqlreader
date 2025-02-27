package sqlreader

import (
	"embed"
	"fmt"
	"strings"
)

// queryStore holds all loaded SQL queries
type queryStore struct {
	queries map[string]string
}

// newQueryStore creates a new query store and loads all SQL queries
func newQueryStore(fs embed.FS, dirPath string) (*queryStore, error) {
	qs := &queryStore{
		queries: make(map[string]string),
	}

	// Read files from the embedded filesystem
	entries, err := fs.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("reading SQL directory: %w", err)
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sql") && !entry.IsDir() {
			content, err := fs.ReadFile(dirPath + "/" + entry.Name())
			if err != nil {
				return nil, fmt.Errorf("reading SQL file %s: %w", entry.Name(), err)
			}

			if err := qs.parseQueries(string(content)); err != nil {
				return nil, fmt.Errorf("parsing queries from %s: %w", entry.Name(), err)
			}
		}
	}

	return qs, nil
}

// parseQueries parses SQL queries from a file content
func (qs *queryStore) parseQueries(content string) error {
	queries := strings.Split(content, "\n\n")
	for _, query := range queries {
		if strings.TrimSpace(query) == "" {
			continue
		}

		lines := strings.Split(query, "\n")
		if len(lines) < 2 {
			continue
		}

		// Parse query name from comment
		nameLine := strings.TrimSpace(lines[0])
		if !strings.HasPrefix(nameLine, "-- name:") {
			continue
		}

		parts := strings.Fields(nameLine)
		if len(parts) < 3 {
			continue
		}

		name := parts[2]
		queryText := strings.Join(lines[1:], "\n")
		qs.queries[name] = strings.TrimSpace(queryText)
	}

	return nil
}

// get returns the SQL query for the given name
// Panics if the query is not found
func (qs *queryStore) get(name string) string {
	query, ok := qs.queries[name]
	if !ok {
		panic(fmt.Sprintf("SQL query %q not found", name))
	}
	return query
}
