-- ============================================================================
-- Extensions
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Tags Table
-- ============================================================================

CREATE TABLE tags (
    id SERIAL PRIMARY KEY, -- Internal primary key for relationships
    uuid UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE, -- Public-facing identifier for API use
    name TEXT NOT NULL UNIQUE, -- Tag name (must be unique)
    description TEXT, -- Optional tag description
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- Record last update timestamp
);

-- Attach update timestamp trigger to tags table (using helper function from create_images.sql)
CREATE TRIGGER update_tags_updated_at
BEFORE UPDATE ON tags
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Tag Closure Table
-- ============================================================================

CREATE TABLE tag_closure (
    ancestor INT NOT NULL, -- Reference to parent/ancestor tag
    descendant INT NOT NULL, -- Reference to child/descendant tag
    depth INT NOT NULL, -- Distance between ancestor and descendant
    PRIMARY KEY (ancestor, descendant), -- Composite primary key
    FOREIGN KEY (ancestor) REFERENCES tags(id) ON DELETE CASCADE, -- Auto-delete when ancestor is removed
    FOREIGN KEY (descendant) REFERENCES tags(id) ON DELETE CASCADE, -- Auto-delete when descendant is removed
    CONSTRAINT chk_tag_closure_no_self_reference CHECK (ancestor <> descendant) -- Prevent self-reference
);

-- Indexes for efficient hierarchical queries
CREATE INDEX idx_tag_closure_ancestor ON tag_closure (ancestor);
CREATE INDEX idx_tag_closure_descendant ON tag_closure (descendant);

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