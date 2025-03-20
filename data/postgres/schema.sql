-- ============================================================================
-- Extensions
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

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
-- Collections Table
-- ============================================================================

-- Create collections table
CREATE TABLE collections (
    id SERIAL PRIMARY KEY, -- Internal primary key for relationships
    uuid UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE, -- Public-facing identifier for API use
    title TEXT NOT NULL, -- Collection title
    description TEXT, -- Optional collection description
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- Record last update timestamp
);

-- Attach update timestamp trigger to collections table
CREATE TRIGGER update_collections_updated_at
BEFORE UPDATE ON collections
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Function to update the parent collection's timestamp when its contents change
CREATE OR REPLACE FUNCTION update_collections_from_link() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE collections SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.collection_id;
    ELSE
        UPDATE collections SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.collection_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Image-Collection Association Table
-- ============================================================================

-- Create image_collections table
CREATE TABLE image_collections (
    collection_id INT NOT NULL, -- Reference to parent collection
    image_id INT NOT NULL, -- Reference to associated image
    position INT NOT NULL, -- Order position within the collection
    PRIMARY KEY (collection_id, image_id), -- Composite primary key
    FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE, -- Auto-delete association when collection is deleted
    FOREIGN KEY (image_id) REFERENCES images(id) ON DELETE SET NULL, -- Maintain collection when image is deleted
    CONSTRAINT chk_image_collections_position_positive CHECK (position > 0), -- Ensure position is positive
    CONSTRAINT unique_position_per_collection UNIQUE (collection_id, position) -- Prevent duplicate positions
);

-- Index for efficient image lookup (collection_id is already indexed as part of the PK)
CREATE INDEX idx_image_collections_image_id ON image_collections (image_id);

-- Compound index for efficient ordering within collections
CREATE INDEX idx_image_collections_collection_position ON image_collections (collection_id, position);

-- Trigger to update collection timestamps when images are added/removed
CREATE TRIGGER trg_update_collections_from_image_collections
AFTER INSERT OR UPDATE OR DELETE ON image_collections
FOR EACH ROW
EXECUTE FUNCTION update_collections_from_link();

-- Function to get the next available position for a new item in a collection
CREATE OR REPLACE FUNCTION get_next_position(p_collection_id INT) 
RETURNS INT AS $$
DECLARE
    next_pos INT;
BEGIN
    -- Find the highest current position and add 1, or use 1 if collection is empty
    SELECT COALESCE(MAX(position) + 1, 1)
    INTO next_pos
    FROM image_collections
    WHERE collection_id = p_collection_id;

    RETURN next_pos;
END;
$$ LANGUAGE plpgsql;

-- Function to maintain the integrity of position values when items are added, moved, or removed
CREATE OR REPLACE FUNCTION reorder_collection_items() RETURNS TRIGGER AS $$
DECLARE
    old_pos INT;
    new_pos INT;
    affected_collection_id INT;
BEGIN
    -- Handle INSERT operations
    IF (TG_OP = 'INSERT') THEN
        affected_collection_id := NEW.collection_id;
        new_pos := NEW.position;
        
        -- Shift existing items down to make room for the new item
        UPDATE image_collections
        SET position = position + 1
        WHERE collection_id = affected_collection_id
          AND image_id != NEW.image_id
          AND position >= new_pos;
          
        RETURN NEW;

    -- Handle UPDATE operations
    ELSIF (TG_OP = 'UPDATE') THEN
        -- If collection_id changed, treat as move to new collection
        IF (OLD.collection_id != NEW.collection_id) THEN
            -- Move items in the old collection up to fill the gap
            UPDATE image_collections
            SET position = position - 1
            WHERE collection_id = OLD.collection_id
              AND position > OLD.position;
              
            -- Move items in new collection down to make space
            UPDATE image_collections
            SET position = position + 1
            WHERE collection_id = NEW.collection_id
              AND position >= NEW.position;
              
            RETURN NEW;
        
        -- If only position changed within the same collection
        ELSIF (OLD.position != NEW.position) THEN
            old_pos := OLD.position;
            new_pos := NEW.position;
            affected_collection_id := NEW.collection_id;
            
            -- Moving item to a later position
            IF (old_pos < new_pos) THEN
                UPDATE image_collections
                SET position = position - 1
                WHERE collection_id = affected_collection_id
                  AND image_id != NEW.image_id
                  AND position > old_pos 
                  AND position <= new_pos;
            
            -- Moving item to an earlier position
            ELSIF (old_pos > new_pos) THEN
                UPDATE image_collections
                SET position = position + 1
                WHERE collection_id = affected_collection_id
                  AND image_id != NEW.image_id
                  AND position >= new_pos 
                  AND position < old_pos;
            END IF;
            
            RETURN NEW;
        END IF;
    
    -- Handle DELETE operations
    ELSIF (TG_OP = 'DELETE') THEN
        -- Shift all higher positions down by 1
        UPDATE image_collections
        SET position = position - 1
        WHERE collection_id = OLD.collection_id
          AND position > OLD.position;
          
        RETURN OLD;
    END IF;
    
    RETURN NULL; -- Should never reach here
END;
$$ LANGUAGE plpgsql;

-- Trigger to maintain position integrity in collections
CREATE TRIGGER reorder_collection_items_trigger
BEFORE INSERT OR UPDATE OR DELETE ON image_collections
FOR EACH ROW
EXECUTE FUNCTION reorder_collection_items();

-- Utility function to move an item to a specific position within a collection
CREATE OR REPLACE FUNCTION move_collection_item(
    p_collection_id INT,
    p_image_id INT,
    p_new_position INT
) RETURNS VOID AS $$
BEGIN
    -- This will trigger the reordering logic automatically
    UPDATE image_collections
    SET position = p_new_position
    WHERE collection_id = p_collection_id
      AND image_id = p_image_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Tags Table
-- ============================================================================

-- Function to prevent circular references in canonical tag relationships
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
        SELECT canonical_id INTO current_id
        FROM tags
        WHERE id = current_id;
        
        depth := depth + 1;
    END LOOP;
    
    -- If a cycle was detected or we hit the max depth, prevent the update
    IF cycle_detected OR depth >= max_depth THEN
        RAISE EXCEPTION 'Circular reference detected in tags canonical_id chain';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create collections table
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

-- Attach update timestamp trigger to tags table
CREATE TRIGGER update_tags_updated_at
BEFORE UPDATE ON tags
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Index for efficient canonical tag lookups
CREATE INDEX idx_tags_canonical_id ON tags (canonical_id);

-- ============================================================================
-- Tag Closure Table
-- ============================================================================

-- Create tag_closure table
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

-- Create image_tags table
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

-- ============================================================================
-- People Table
-- ============================================================================

-- Create people table
CREATE TABLE people (
    id SERIAL PRIMARY KEY, -- Internal primary key for relationships
    uuid UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE, -- Public-facing identifier for API use
    name TEXT NOT NULL UNIQUE, -- Person's name
    description TEXT, -- Optional person description
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- Record last update timestamp
);

-- Attach update timestamp trigger to people table
CREATE TRIGGER update_people_updated_at
BEFORE UPDATE ON people
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Function to update the parent person's timestamp when a related entity is modified
CREATE OR REPLACE FUNCTION update_people_from_link() RETURNS trigger AS $$
BEGIN
    -- Handle both insert/update and delete operations
    IF TG_OP = 'DELETE' THEN
        -- Only update timestamp if it's older than current time to prevent unnecessary updates
        UPDATE people SET updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.person_id AND updated_at < CURRENT_TIMESTAMP;
    ELSE
        -- Only update timestamp if it's older than current time to prevent unnecessary updates
        UPDATE people SET updated_at = CURRENT_TIMESTAMP
        WHERE id = NEW.person_id AND updated_at < CURRENT_TIMESTAMP;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Image-People Association Table
-- ============================================================================

-- Define the roles a person can have in relation to an image
CREATE TYPE image_role AS ENUM ('creator', 'subject');

-- Create image_people table
CREATE TABLE image_people (
    image_id INT NOT NULL, -- Reference to associated image
    person_id INT NOT NULL, -- Reference to associated person
    role image_role NOT NULL, -- Person's role (creator/subject)
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    PRIMARY KEY (image_id, person_id, role), -- Composite primary key (allows one person to have multiple roles)
    FOREIGN KEY (image_id) REFERENCES images(id) ON DELETE CASCADE, -- Auto-delete when image is removed
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE -- Auto-delete when person is removed
);

-- Trigger to update image timestamps when people are added/removed
CREATE TRIGGER trg_update_images_from_image_people
AFTER INSERT OR UPDATE OR DELETE ON image_people
FOR EACH ROW
EXECUTE FUNCTION update_images_from_link();

-- Index for efficient person lookup
CREATE INDEX idx_image_people_person_id ON image_people (person_id);

-- ============================================================================
-- Sources Table
-- ============================================================================

-- Create image_sources table
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

-- Create person_sources table
CREATE TABLE person_sources (
    id SERIAL PRIMARY KEY, -- Internal primary key for relationships
    person_id INT NOT NULL, -- Reference to associated person
    url TEXT NOT NULL, -- Source URL
    title TEXT, -- Optional source title
    description TEXT, -- Optional source description
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record last update timestamp
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE, -- Auto-delete when person is removed
    CONSTRAINT person_sources_unique_url UNIQUE (person_id, url) -- Prevent duplicate URLs for the same image
);

-- Attach update timestamp trigger to person_sources table
CREATE TRIGGER update_person_sources_updated_at
BEFORE UPDATE ON person_sources
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Trigger to update image timestamps when sources are added/removed
CREATE TRIGGER trg_update_people_from_person_sources
AFTER INSERT OR UPDATE OR DELETE ON person_sources
FOR EACH ROW
EXECUTE FUNCTION update_people_from_link();