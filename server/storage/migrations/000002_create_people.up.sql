-- ============================================================================
-- Extensions
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

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

-- Attach update timestamp trigger to people table (using the helper function from create_images.sql)
CREATE TRIGGER update_people_updated_at
BEFORE UPDATE ON people
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- People Helper Function
-- ============================================================================

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
-- Image-People Association
-- ============================================================================

-- Define the roles a person can have in relation to an image
CREATE TYPE image_role AS ENUM ('creator', 'subject');

-- Create image_people table to associate people with images
CREATE TABLE image_people (
    image_id INT NOT NULL, -- Reference to associated image
    person_id INT NOT NULL, -- Reference to associated person
    role image_role NOT NULL, -- Person's role (creator/subject)
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- Record creation timestamp
    PRIMARY KEY (image_id, person_id, role), -- Composite primary key (allows one person to have multiple roles)
    FOREIGN KEY (image_id) REFERENCES images(id) ON DELETE CASCADE, -- Auto-delete when image is removed
    FOREIGN KEY (person_id) REFERENCES people(id) ON DELETE CASCADE -- Auto-delete when person is removed
);

-- Trigger to update image timestamps when people associations change
CREATE TRIGGER trg_update_images_from_image_people
AFTER INSERT OR UPDATE OR DELETE ON image_people
FOR EACH ROW
EXECUTE FUNCTION update_images_from_link();

-- Index for efficient lookup of people in associations
CREATE INDEX idx_image_people_person_id ON image_people (person_id);

-- ============================================================================
-- Person Sources Table
-- ============================================================================

-- Create person_sources table to hold external references for people
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

-- Trigger to update people timestamps when sources are added/removed
CREATE TRIGGER trg_update_people_from_person_sources
AFTER INSERT OR UPDATE OR DELETE ON person_sources
FOR EACH ROW
EXECUTE FUNCTION update_people_from_link();