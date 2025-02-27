-- name: create_user
INSERT INTO users (username, name)
VALUES ($1, $2)
RETURNING id

-- name: get_user_by_username
SELECT id, username, name
FROM users
WHERE username = $1

-- name: list_users
SELECT id, username, name
FROM users
ORDER BY id

-- name: update_user_preferences
UPDATE users
SET preferences = $1::jsonb
WHERE username = $2 