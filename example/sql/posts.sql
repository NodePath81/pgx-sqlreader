-- name: create_post
INSERT INTO posts (user_id, title, content)
VALUES ($1, $2, $3)
RETURNING id

-- name: get_post_by_id
SELECT p.id, p.title, p.content, p.created_at, u.username, u.name
FROM posts p
JOIN users u ON p.user_id = u.id
WHERE p.id = $1

-- name: list_user_posts
SELECT id, title, content, created_at
FROM posts
WHERE user_id = $1
ORDER BY created_at DESC 