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
    parent_id INT, -- Parent ID to enable unique position constraint
    position INT NOT NULL, -- Gap based (e.g. 10,20,30) to provide room for insertions
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record last update timestamp
    FOREIGN KEY (parent_id) REFERENCES tags(id) ON DELETE SET NULL,
    CONSTRAINT tags_unique_parent_id_position UNIQUE (parent_id, position) DEFERRABLE INITIALLY IMMEDIATE -- Prevent duplicate positions for the same parent_id
);

-- Attach update timestamp trigger to tags table (using helper function from create_images.sql)
CREATE TRIGGER update_tags_updated_at
BEFORE UPDATE ON tags
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Index the sort_order and parent_id fields for performance reasons
CREATE INDEX idx_tags_parent_sort_order ON tags (parent_id, sort_order);

-- ============================================================================
-- Tag Closure Table
-- ============================================================================

CREATE TABLE tag_closure (
    ancestor INT NOT NULL, -- Reference to parent/ancestor tag
    descendant INT NOT NULL, -- Reference to child/descendant tag
    depth INT NOT NULL, -- Distance between ancestor and descendant
    PRIMARY KEY (ancestor, descendant), -- Composite primary key
    FOREIGN KEY (ancestor) REFERENCES tags(id) ON DELETE CASCADE, -- Auto-delete when ancestor is removed
    FOREIGN KEY (descendant) REFERENCES tags(id) ON DELETE CASCADE -- Auto-delete when descendant is removed
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