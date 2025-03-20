-- ============================================================================
-- Extensions
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

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

-- ============================================================================
-- Helper Function: Update Collections from Link
-- ============================================================================

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

-- Create image_collections table to associate images with collections
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

-- Index for efficient image lookup (collection_id is part of the primary key)
CREATE INDEX idx_image_collections_image_id ON image_collections (image_id);

-- Compound index for efficient ordering within collections
CREATE INDEX idx_image_collections_collection_position ON image_collections (collection_id, position);

-- Trigger to update collection timestamps when images are added/removed
CREATE TRIGGER trg_update_collections_from_image_collections
AFTER INSERT OR UPDATE OR DELETE ON image_collections
FOR EACH ROW
EXECUTE FUNCTION update_collections_from_link();

-- ============================================================================
-- Helper Function: Get Next Position
-- ============================================================================

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

-- ============================================================================
-- Helper Function: Reorder Collection Items
-- ============================================================================

-- Function to maintain the integrity of position values when items are added, moved, or removed
CREATE OR REPLACE FUNCTION reorder_collection_items() RETURNS TRIGGER AS $$
DECLARE
    old_pos INT;
    new_pos INT;
    affected_collection_id INT;
BEGIN
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
    ELSIF (TG_OP = 'UPDATE') THEN
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
    ELSIF (TG_OP = 'DELETE') THEN
        -- Shift all higher positions down by 1
        UPDATE image_collections
        SET position = position - 1
        WHERE collection_id = OLD.collection_id
          AND position > OLD.position;
          
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger to maintain position integrity in collections
CREATE TRIGGER reorder_collection_items_trigger
BEFORE INSERT OR UPDATE OR DELETE ON image_collections
FOR EACH ROW
EXECUTE FUNCTION reorder_collection_items();

-- ============================================================================
-- Utility Function: Move Collection Item
-- ============================================================================

-- Function to move an item to a specific position within a collection; the reordering logic triggers automatically
CREATE OR REPLACE FUNCTION move_collection_item(
    p_collection_id INT,
    p_image_id INT,
    p_new_position INT
) RETURNS VOID AS $$
BEGIN
    UPDATE image_collections SET position = p_new_position WHERE collection_id = p_collection_id AND image_id = p_image_id;
END;
$$ LANGUAGE plpgsql;
