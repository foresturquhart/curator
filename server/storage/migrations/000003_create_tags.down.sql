DROP TRIGGER IF EXISTS trg_update_images_from_image_tags ON image_tags;
DROP TABLE IF EXISTS image_tags;
DROP INDEX IF EXISTS idx_tags_parent_position;
DROP TRIGGER IF EXISTS update_tags_updated_at ON tags;
DROP TABLE IF EXISTS tags;