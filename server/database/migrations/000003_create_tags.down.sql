DROP TRIGGER IF EXISTS trg_update_images_from_image_tags ON image_tags;
DROP TABLE IF EXISTS image_tags;
DROP INDEX IF EXISTS idx_tag_closure_descendant;
DROP INDEX IF EXISTS idx_tag_closure_ancestor;
DROP TABLE IF EXISTS tag_closure;
DROP INDEX IF EXISTS idx_tags_parent_sort_order;
DROP TRIGGER IF EXISTS update_tags_updated_at ON tags;
DROP TABLE IF EXISTS tags;