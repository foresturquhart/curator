-- ============================================================================
-- Extensions
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Tags Table
-- ============================================================================

CREATE TABLE tags (
    id SERIAL PRIMARY KEY, -- 
    parent_id INT REFERENCES tags(id) ON DELETE RESTRICT,
    uuid UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    position INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tags_unique_parent_id_position UNIQUE (parent_id, position) DEFERRABLE INITIALLY IMMEDIATE
);

-- Index the position and parent_id fields for performance reasons
CREATE INDEX idx_tags_parent_id_position ON tags (parent_id, position);

-- Attach update timestamp trigger to tags table (using helper function from create_images.sql)
CREATE TRIGGER update_tags_updated_at BEFORE UPDATE ON tags FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Image-Tag Association Table
-- ============================================================================

CREATE TABLE image_tags (
    image_id INT NOT NULL, -- Reference to associated image
    tag_id INT NOT NULL, -- Reference to associated tag
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    PRIMARY KEY (image_id, tag_id), -- Composite primary key
    FOREIGN KEY (image_id) REFERENCES images(id) ON DELETE CASCADE, -- Auto-delete when image is removed
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE -- Auto-delete when tag is removed
);

-- Trigger to update image timestamps when tags are added/removed
CREATE TRIGGER trg_update_images_from_image_tags
AFTER INSERT OR UPDATE OR DELETE ON image_tags
FOR EACH ROW
EXECUTE FUNCTION update_images_from_link();