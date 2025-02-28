-- Create posts table with foreign key to users
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index on user_id for efficient querying
CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts USING btree (user_id);

-- Down
DROP INDEX IF EXISTS idx_posts_user_id;
DROP TABLE IF EXISTS posts; 