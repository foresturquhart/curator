-- ============================================================================
-- Extensions
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- Tags Domain
-- ============================================================================

-- ============================================================================
-- Helper Function: Prevent Circular Tag References
-- ============================================================================

CREATE OR REPLACE FUNCTION prevent_circular_tag_references() RETURNS TRIGGER AS $$
DECLARE
    current_id INT;
    cycle_detected BOOLEAN := FALSE;
    depth INT := 0;
    max_depth CONSTANT INT := 100; -- Safety limit to prevent infinite loops
BEGIN
    -- Only perform check if canonical_id is being set
    IF NEW.canonical_id IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Start with the canonical_id that's being set
    current_id := NEW.canonical_id;
    
    -- Follow the chain of canonical_ids to detect cycles
    WHILE current_id IS NOT NULL AND depth < max_depth AND NOT cycle_detected LOOP
        -- If we've reached the original tag, we have a cycle
        IF current_id = NEW.id THEN
            cycle_detected := TRUE;
        END IF;
        
        -- Get the next canonical_id in the chain
        SELECT canonical_id INTO current_id FROM tags WHERE id = current_id;
        
        depth := depth + 1;
    END LOOP;
    
    -- If a cycle was detected or we hit the max depth, prevent the update
    IF cycle_detected OR depth >= max_depth THEN
        RAISE EXCEPTION 'Circular reference detected in tags canonical_id chain';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Tags Table
-- ============================================================================

CREATE TABLE tags (
    id SERIAL PRIMARY KEY, -- Internal primary key for relationships
    uuid UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE, -- Public-facing identifier for API use
    canonical_id INT, -- Optional reference to canonical form of this tag
    name TEXT NOT NULL UNIQUE, -- Tag name (must be unique)
    description TEXT, -- Optional tag description
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record last update timestamp
    FOREIGN KEY (canonical_id) REFERENCES tags(id), -- Self-reference to canonical tag
    CONSTRAINT chk_tags_no_self_reference CHECK (canonical_id IS NULL OR canonical_id <> id) -- Prevent self-reference
);

-- Trigger to prevent circular canonical references
CREATE TRIGGER prevent_circular_tag_references_trigger
BEFORE INSERT OR UPDATE ON tags
FOR EACH ROW
EXECUTE FUNCTION prevent_circular_tag_references();

-- Attach update timestamp trigger to tags table (using helper function from create_images.sql)
CREATE TRIGGER update_tags_updated_at
BEFORE UPDATE ON tags
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Index for efficient canonical tag lookups
CREATE INDEX idx_tags_canonical_id ON tags (canonical_id);

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