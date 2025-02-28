-- name: create_comment
INSERT INTO comments (post_id, user_id, content)
VALUES ($1, $2, $3)
RETURNING id

-- name: get_post_comments
SELECT c.id, c.content, c.created_at, u.username, u.name
FROM comments c
JOIN users u ON c.user_id = u.id
WHERE c.post_id = $1
ORDER BY c.created_at ASC

-- name: count_post_comments
SELECT COUNT(*)
FROM comments
WHERE post_id = $1 