DROP TRIGGER IF EXISTS trg_update_images_from_image_sources ON image_sources;
DROP TRIGGER IF EXISTS update_image_sources_updated_at ON image_sources;
DROP TABLE IF EXISTS image_sources;
DROP TRIGGER IF EXISTS update_images_updated_at ON images;
DROP TABLE IF EXISTS images;
DROP TYPE IF EXISTS image_format;
DROP FUNCTION IF EXISTS update_images_from_link();
DROP FUNCTION IF EXISTS update_updated_at_column();