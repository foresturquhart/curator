-- ============================================================================
-- Extensions
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- Automatically updates the updated_at timestamp column on record changes
CREATE OR REPLACE FUNCTION update_updated_at_column() RETURNS trigger AS $$
BEGIN
    -- Only update the timestamp if this is an UPDATE operation and the row data actually changed
    IF (TG_OP = 'UPDATE' AND OLD IS DISTINCT FROM NEW) THEN
        NEW.updated_at = CURRENT_TIMESTAMP;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Images Table
-- ============================================================================

-- Define valid image formats as an enumeration
CREATE TYPE image_format AS ENUM ('jpeg', 'png', 'gif', 'bmp');

-- Create images table
CREATE TABLE images (
    id SERIAL PRIMARY KEY, -- Internal primary key for relationships
    uuid UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE, -- Public-facing identifier for API use
    filename TEXT NOT NULL, -- Original filename of the image
    md5 TEXT NOT NULL UNIQUE, -- MD5 hash for deduplication and integrity checks
    sha1 TEXT NOT NULL UNIQUE, -- SHA1 hash for additional integrity verification
    width INT CONSTRAINT chk_images_width_positive CHECK (width > 0), -- Image width in pixels
    height INT CONSTRAINT chk_images_height_positive CHECK (height > 0), -- Image height in pixels
    format image_format NOT NULL, -- File format
    size BIGINT CONSTRAINT chk_images_size_positive CHECK (size > 0), -- File size in bytes
    embedding vector(512) NOT NULL, -- Vector embedding for similarity search
    title TEXT, -- Optional user-provided title
    description TEXT, -- Optional user-provided description
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- Record last update timestamp
);

-- Attach update timestamp trigger to images table
CREATE TRIGGER update_images_updated_at
BEFORE UPDATE ON images
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Function to update the parent image's timestamp when a related entity is modified
CREATE OR REPLACE FUNCTION update_images_from_link() RETURNS trigger AS $$
BEGIN
    -- Handle both insert/update and delete operations
    IF TG_OP = 'DELETE' THEN
        -- Only update timestamp if it's older than current time to prevent unnecessary updates
        UPDATE images SET updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.image_id AND updated_at < CURRENT_TIMESTAMP;
    ELSE
        -- Only update timestamp if it's older than current time to prevent unnecessary updates
        UPDATE images SET updated_at = CURRENT_TIMESTAMP
        WHERE id = NEW.image_id AND updated_at < CURRENT_TIMESTAMP;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Image Sources Table
-- ============================================================================

-- Create image_sources table which is solely dependent on images
CREATE TABLE image_sources (
    id SERIAL PRIMARY KEY, -- Internal primary key for relationships
    image_id INT NOT NULL, -- Reference to associated image
    url TEXT NOT NULL, -- Source URL
    title TEXT, -- Optional source title
    description TEXT, -- Optional source description
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record last update timestamp
    FOREIGN KEY (image_id) REFERENCES images(id) ON DELETE CASCADE, -- Auto-delete when image is removed
    CONSTRAINT image_sources_unique_url UNIQUE (image_id, url) -- Prevent duplicate URLs for the same image
);

-- Attach update timestamp trigger to image_sources table
CREATE TRIGGER update_image_sources_updated_at
BEFORE UPDATE ON image_sources
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Trigger to update image timestamps when sources are added/removed
CREATE TRIGGER trg_update_images_from_image_sources
AFTER INSERT OR UPDATE OR DELETE ON image_sources
FOR EACH ROW
EXECUTE FUNCTION update_images_from_link();