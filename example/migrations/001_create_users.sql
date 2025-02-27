-- Create users table with JSONB support
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    preferences JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index on JSONB field for efficient querying
CREATE INDEX IF NOT EXISTS idx_users_preferences ON users USING gin (preferences);

-- Down
DROP INDEX IF EXISTS idx_users_preferences;
DROP TABLE IF EXISTS users; 