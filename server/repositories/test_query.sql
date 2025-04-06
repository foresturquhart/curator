WITH RECURSIVE tag_tree AS (
  -- Base case: select root nodes (nodes with no parent)
  SELECT
    id,
    uuid,
    name,
    description,
    position,
    parent_id,
    0 AS depth,
    LPAD(position::text, 5, '0') AS sort_path
  FROM tags
  WHERE parent_id IS NULL

  UNION ALL

  -- Recursive case: join each parent with its immediate children
  SELECT
    t.id,
    t.uuid,
    t.name,
    t.description,
    t.position,
    t.parent_id,
    tt.depth + 1 AS depth,
    tt.sort_path || '.' || LPAD(t.position::text, 5, '0') AS sort_path
  FROM tags t
  JOIN tag_tree tt ON t.parent_id = tt.id
)
SELECT 
  repeat('    ', depth) || name AS tree_visual,
  id,
  name,
  position,
  depth,
  parent_id,
  sort_path,
  description,
  uuid
FROM tag_tree
ORDER BY sort_path;