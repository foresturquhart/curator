/**
 * Function: insert_tag_inside
 * 
 * Inserts a new tag at the beginning (position 0) of a parent's children list,
 * shifting all existing children to higher positions.
 *
 * Parameters:
 *   @p_parent_id - The ID of the parent tag (NULL for root-level tags)
 *   @p_name - The name of the new tag
 *   @p_description - Optional description for the new tag
 *
 * Returns: 
 *   All columns of the newly created tag record
 *
 * Transaction Safety:
 *   Uses advisory lock to prevent concurrent modifications of the same parent's children
 */
CREATE OR REPLACE FUNCTION insert_tag_inside(
    p_parent_id INTEGER,
    p_name TEXT,
    p_description TEXT
)
RETURNS TABLE (
    id INTEGER,
    uuid UUID,
    name TEXT,
    description TEXT,
    parent_id INTEGER,
    "position" INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) AS $$
BEGIN
    -- Use value 0 for NULL parent_id to create a separate lock namespace for root tags
    PERFORM pg_advisory_xact_lock(coalesce(p_parent_id, 0));

    -- Shift positions of existing children (if any) upward by 1
    -- IS NOT DISTINCT FROM handles NULL values in parent_id (root level tags)
    UPDATE tags
    SET "position" = "position" + 1
    WHERE parent_id IS NOT DISTINCT FROM p_parent_id;

    -- Insert the new tag at position 0
    RETURN QUERY
        INSERT INTO tags (name, description, parent_id, "position")
        VALUES (p_name, p_description, p_parent_id, 0)
        RETURNING id, uuid, name, description, parent_id, "position", created_at, updated_at;
END;
$$ LANGUAGE plpgsql;

/**
 * Function: insert_tag_before
 * 
 * Inserts a new tag immediately before a specified target tag in the same parent's list,
 * shifting the target tag and subsequent siblings to higher positions.
 *
 * Parameters:
 *   @p_target_id - The ID of the target tag to insert before
 *   @p_name - The name of the new tag
 *   @p_description - Optional description for the new tag
 *
 * Returns: 
 *   All columns of the newly created tag record
 *
 * Transaction Safety:
 *   Uses advisory lock to prevent concurrent modifications of the affected parent's children
 *
 * Exceptions:
 *   Raises an exception if the target tag does not exist
 *
 * Notes:
 *   - The new tag inherits the parent_id from the target tag
 *   - Takes the exact position of the target tag, pushing the target and later siblings up
 */
CREATE OR REPLACE FUNCTION insert_tag_before(
    p_target_id INTEGER,
    p_name TEXT,
    p_description TEXT
)
RETURNS TABLE (
    id INTEGER,
    uuid UUID,
    name TEXT,
    description TEXT,
    parent_id INTEGER,
    "position" INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) AS $$
DECLARE
    v_parent_id INTEGER;       -- Parent ID extracted from the target tag
    v_target_position INTEGER; -- Position extracted from the target tag
BEGIN
    -- First lookup the target tag to get its parent and position
    SELECT parent_id, "position"
    INTO v_parent_id, v_target_position
    FROM tags
    WHERE id = p_target_id;

    -- Validate the target tag exists
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Target tag with id % not found', p_target_id;
    END IF;

    -- Lock the parent to prevent concurrent modifications to its children's ordering
    PERFORM pg_advisory_xact_lock(coalesce(v_parent_id, 0));

    -- Shift positions of siblings at or after the target position upward
    -- This creates a gap at the target position for the new tag
    UPDATE tags
    SET "position" = "position" + 1
    WHERE parent_id IS NOT DISTINCT FROM v_parent_id
      AND "position" >= v_target_position;

    -- Insert the new tag at the position previously occupied by the target tag
    RETURN QUERY
        INSERT INTO tags (name, description, parent_id, "position")
        VALUES (p_name, p_description, v_parent_id, v_target_position)
        RETURNING id, uuid, name, description, parent_id, "position", created_at, updated_at;
END;
$$ LANGUAGE plpgsql;

/**
 * Function: insert_tag_after
 * 
 * Inserts a new tag immediately after a specified target tag in the same parent's list,
 * shifting subsequent siblings to higher positions.
 *
 * Parameters:
 *   @p_target_id - The ID of the target tag to insert after
 *   @p_name - The name of the new tag
 *   @p_description - Optional description for the new tag
 *
 * Returns: 
 *   All columns of the newly created tag record
 *
 * Transaction Safety:
 *   Uses advisory lock to prevent concurrent modifications of the affected parent's children
 *
 * Exceptions:
 *   Raises an exception if the target tag does not exist
 *
 * Notes:
 *   - The new tag inherits the parent_id from the target tag
 *   - The new tag is positioned immediately after the target tag (target_position + 1)
 *   - Only siblings after the target position are shifted up
 */
CREATE OR REPLACE FUNCTION insert_tag_after(
    p_target_id INTEGER,
    p_name TEXT,
    p_description TEXT
)
RETURNS TABLE (
    id INTEGER,
    uuid UUID,
    name TEXT,
    description TEXT,
    parent_id INTEGER,
    "position" INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) AS $$
DECLARE
    v_parent_id INTEGER;       -- Parent ID extracted from the target tag
    v_target_position INTEGER; -- Position extracted from the target tag
BEGIN
    -- First lookup the target tag to get its parent and position
    SELECT parent_id, "position"
    INTO v_parent_id, v_target_position
    FROM tags
    WHERE id = p_target_id;

    -- Validate the target tag exists
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Target tag with id % not found', p_target_id;
    END IF;

    -- Lock the parent to prevent concurrent modifications to its children's ordering
    PERFORM pg_advisory_xact_lock(coalesce(v_parent_id, 0));

    -- Shift positions of siblings that come after the target position
    -- Only need to shift tags with position > target_position (not >=)
    -- since we're inserting after, not before the target
    UPDATE tags
    SET "position" = "position" + 1
    WHERE parent_id IS NOT DISTINCT FROM v_parent_id
      AND "position" > v_target_position;

    -- Insert the new tag immediately after the target tag's position
    RETURN QUERY
        INSERT INTO tags (name, description, parent_id, "position")
        VALUES (p_name, p_description, v_parent_id, v_target_position + 1)
        RETURNING id, uuid, name, description, parent_id, "position", created_at, updated_at;
END;
$$ LANGUAGE plpgsql;

/**
 * Function: move_tag_inside
 * 
 * Moves an existing tag to the beginning (position 0) of a new parent's children list,
 * maintaining ordering in both the source and destination parents.
 *
 * Parameters:
 *   @p_tag_id - The ID of the tag to move
 *   @p_new_parent_id - The ID of the new parent tag (NULL for root level)
 *
 * Returns: 
 *   All columns of the moved tag record with updated parent and position
 *
 * Transaction Safety:
 *   Uses advisory locks on both source and destination parents to prevent concurrent modifications
 *
 * Exceptions:
 *   Raises an exception if the tag to move does not exist
 *
 * Notes:
 *   - Updates positions in both source and destination parent lists
 *   - Can be used to move a tag to the root level by passing NULL for p_new_parent_id
 *   - Works correctly even when moving within the same parent (source parent = destination parent)
 */
CREATE OR REPLACE FUNCTION move_tag_inside(
    p_tag_id INTEGER,
    p_new_parent_id INTEGER
)
RETURNS TABLE (
    id INTEGER,
    uuid UUID,
    name TEXT,
    description TEXT,
    parent_id INTEGER,
    "position" INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) AS $$
DECLARE
    v_old_parent INTEGER;   -- Current parent ID of the tag being moved
    v_old_position INTEGER; -- Current position of the tag being moved
BEGIN
    -- Retrieve the current parent and position of the tag being moved
    SELECT parent_id, "position"
    INTO v_old_parent, v_old_position
    FROM tags
    WHERE id = p_tag_id;

    -- Validate the tag exists
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Tag with id % not found', p_tag_id;
    END IF;

    -- Acquire locks on both the source and destination parent groups
    -- Using 0 for NULL parents (root level tags) creates a distinct lock namespace
    PERFORM pg_advisory_xact_lock(coalesce(v_old_parent, 0));
    PERFORM pg_advisory_xact_lock(coalesce(p_new_parent_id, 0));

    -- In the source parent's list, shift positions down to close the gap
    -- left by the moving tag (only for tags with higher positions)
    UPDATE tags
    SET "position" = "position" - 1
    WHERE parent_id IS NOT DISTINCT FROM v_old_parent
      AND "position" > v_old_position;
    
    -- In the destination parent's list, shift all existing positions up
    -- to make room at the beginning (position 0)
    UPDATE tags
    SET "position" = "position" + 1
    WHERE parent_id IS NOT DISTINCT FROM p_new_parent_id;

    -- Update the moving tag with its new parent and position 0 (first position)
    UPDATE tags
    SET parent_id = p_new_parent_id,
        "position" = 0
    WHERE id = p_tag_id;

    -- Return the updated tag record with all its columns
    RETURN QUERY
        SELECT id, uuid, name, description, parent_id, "position", created_at, updated_at
        FROM tags
        WHERE id = p_tag_id;
END;
$$ LANGUAGE plpgsql;

/**
 * Function: move_tag_before
 * 
 * Moves an existing tag to a position immediately before a specified target tag,
 * maintaining ordering in both the source and destination parents.
 *
 * Parameters:
 *   @p_tag_id - The ID of the tag to move
 *   @p_target_id - The ID of the target tag to position before
 *
 * Returns: 
 *   All columns of the moved tag record with updated parent and position
 *
 * Transaction Safety:
 *   Uses advisory locks on both source and destination parents to prevent concurrent modifications
 *
 * Exceptions:
 *   - Raises an exception if either the tag to move or target tag does not exist
 *   - Raises an exception if attempting to move a tag relative to itself
 *
 * Notes:
 *   - Handles both intra-parent moves (within same parent) and inter-parent moves
 *   - For intra-parent moves, correctly handles the position shift based on relative positions
 *   - The tag will take on the parent of the target tag when moved
 */
CREATE OR REPLACE FUNCTION move_tag_before(
    p_tag_id INTEGER,
    p_target_id INTEGER
)
RETURNS TABLE (
    id INTEGER,
    uuid UUID,
    name TEXT,
    description TEXT,
    parent_id INTEGER,
    "position" INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) AS $$
DECLARE
    v_old_parent INTEGER;      -- Current parent ID of the tag being moved
    v_old_position INTEGER;    -- Current position of the tag being moved
    v_target_parent INTEGER;   -- Parent ID of the target tag
    v_target_position INTEGER; -- Position of the target tag
    v_new_position INTEGER;    -- Calculated new position for the moving tag
BEGIN
    -- Prevent moving a tag relative to itself (invalid operation)
    IF p_tag_id = p_target_id THEN
        RAISE EXCEPTION 'Cannot move a tag relative to itself';
    END IF;

    -- Retrieve current parent and position of the tag being moved
    SELECT parent_id, "position"
    INTO v_old_parent, v_old_position
    FROM tags
    WHERE id = p_tag_id;

    -- Validate the tag to move exists
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Tag with id % not found', p_tag_id;
    END IF;

    -- Retrieve parent and position of the target tag
    SELECT parent_id, "position"
    INTO v_target_parent, v_target_position
    FROM tags
    WHERE id = p_target_id;
    
    -- Validate the target tag exists
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Target tag with id % not found', p_target_id;
    END IF;

    -- Acquire locks on both source and destination parents to prevent concurrent modifications
    PERFORM pg_advisory_xact_lock(coalesce(v_old_parent, 0));
    PERFORM pg_advisory_xact_lock(coalesce(v_target_parent, 0));

    -- Handle differently depending on whether this is a same-parent or cross-parent move
    IF v_old_parent IS NOT DISTINCT FROM v_target_parent THEN
        -- CASE 1: Moving within the same parent (intra-parent move)
        
        -- Calculate the new position based on relative positions:
        -- If moving tag is currently before the target, we need to account for the shift
        -- that will happen when we remove the moving tag
        IF v_old_position < v_target_position THEN
            -- When moving forward in the list, the target's position will shift down by 1
            -- after we remove the moving tag, so we need to adjust
            v_new_position := v_target_position - 1;
        ELSE
            -- When moving backward in the list, the target's position doesn't change
            -- when we remove the moving tag
            v_new_position := v_target_position;
        END IF;
        
        -- First remove the moving tag from its current position by shifting down
        -- all higher positioned tags to close the gap
        UPDATE tags
        SET "position" = "position" - 1
        WHERE parent_id IS NOT DISTINCT FROM v_old_parent
          AND "position" > v_old_position;
        
        -- Then make room at the new position by shifting up all tags at or after
        -- the insertion point
        UPDATE tags
        SET "position" = "position" + 1
        WHERE parent_id IS NOT DISTINCT FROM v_target_parent
          AND "position" >= v_new_position;
    ELSE
        -- CASE 2: Moving to a different parent (inter-parent move)
        
        -- Close the gap in the source parent by shifting down
        UPDATE tags
        SET "position" = "position" - 1
        WHERE parent_id IS NOT DISTINCT FROM v_old_parent
          AND "position" > v_old_position;
        
        -- Make room in the destination parent by shifting up
        UPDATE tags
        SET "position" = "position" + 1
        WHERE parent_id IS NOT DISTINCT FROM v_target_parent
          AND "position" >= v_target_position;
        
        -- When moving to a different parent, we use the target position directly
        v_new_position := v_target_position; 
    END IF;

    -- Update the moving tag with the new parent and new position
    UPDATE tags
    SET parent_id = v_target_parent,
        "position" = v_new_position
    WHERE id = p_tag_id;

    -- Return the updated tag record with all its columns
    RETURN QUERY
        SELECT id, uuid, name, description, parent_id, "position", created_at, updated_at
        FROM tags
        WHERE id = p_tag_id;
END;
$$ LANGUAGE plpgsql;

/**
 * Function: move_tag_after
 * 
 * Moves an existing tag to a position immediately after a specified target tag,
 * maintaining ordering in both the source and destination parents.
 *
 * Parameters:
 *   @p_tag_id - The ID of the tag to move
 *   @p_target_id - The ID of the target tag to position after
 *
 * Returns: 
 *   All columns of the moved tag record with updated parent and position
 *
 * Transaction Safety:
 *   Uses advisory locks on both source and destination parents to prevent concurrent modifications
 *
 * Exceptions:
 *   - Raises an exception if either the tag to move or target tag does not exist
 *   - Raises an exception if attempting to move a tag relative to itself
 *
 * Notes:
 *   - Handles both intra-parent moves (within same parent) and inter-parent moves
 *   - For intra-parent moves, correctly handles the position shift based on relative positions
 *   - The tag will take on the parent of the target tag when moved
 */
CREATE OR REPLACE FUNCTION move_tag_after(
    p_tag_id INTEGER,
    p_target_id INTEGER
)
RETURNS TABLE (
    id INTEGER,
    uuid UUID,
    name TEXT,
    description TEXT,
    parent_id INTEGER,
    "position" INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
) AS $$
DECLARE
    v_old_parent INTEGER;      -- Current parent ID of the tag being moved
    v_old_position INTEGER;    -- Current position of the tag being moved
    v_target_parent INTEGER;   -- Parent ID of the target tag
    v_target_position INTEGER; -- Position of the target tag
    v_new_position INTEGER;    -- Calculated new position for the moving tag
BEGIN
    -- Prevent moving a tag relative to itself (invalid operation)
    IF p_tag_id = p_target_id THEN
        RAISE EXCEPTION 'Cannot move a tag relative to itself';
    END IF;

    -- Retrieve current parent and position of the tag being moved
    SELECT parent_id, "position"
    INTO v_old_parent, v_old_position
    FROM tags
    WHERE id = p_tag_id;

    -- Validate the tag to move exists
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Tag with id % not found', p_tag_id;
    END IF;

    -- Retrieve parent and position of the target tag
    SELECT parent_id, "position"
    INTO v_target_parent, v_target_position
    FROM tags
    WHERE id = p_target_id;
    
    -- Validate the target tag exists
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Target tag with id % not found', p_target_id;
    END IF;

    -- Acquire locks on both source and destination parents to prevent concurrent modifications
    PERFORM pg_advisory_xact_lock(coalesce(v_old_parent, 0));
    PERFORM pg_advisory_xact_lock(coalesce(v_target_parent, 0));

    -- Handle differently depending on whether this is a same-parent or cross-parent move
    IF v_old_parent IS NOT DISTINCT FROM v_target_parent THEN
        -- CASE 1: Moving within the same parent (intra-parent move)
                
        -- Calculate the new position based on relative positions:
        IF v_old_position < v_target_position THEN
            -- When moving forward in the list, we want position = target_position
            -- The moving tag will end up directly after the target tag
            v_new_position := v_target_position;
        ELSE
            -- When moving backward in the list, we need target_position + 1
            -- Because after removing the moving tag, positions shift
            v_new_position := v_target_position + 1;
        END IF;
        
        -- First remove the moving tag from its current position by shifting down
        -- all higher positioned tags to close the gap
        UPDATE tags
        SET "position" = "position" - 1
        WHERE parent_id IS NOT DISTINCT FROM v_old_parent
          AND "position" > v_old_position;
        
        -- Then make room at the new position by shifting up all tags at or after
        -- the insertion point
        UPDATE tags
        SET "position" = "position" + 1
        WHERE parent_id IS NOT DISTINCT FROM v_target_parent
          AND "position" >= v_new_position;
    ELSE
        -- CASE 2: Moving to a different parent (inter-parent move)
        
        -- Close the gap in the source parent by shifting down
        UPDATE tags
        SET "position" = "position" - 1
        WHERE parent_id IS NOT DISTINCT FROM v_old_parent
          AND "position" > v_old_position;
        
        -- Make room in the destination parent by shifting up
        -- Note that we only need to shift tags AFTER the target position (> not >=)
        -- since we're inserting after the target
        UPDATE tags
        SET "position" = "position" + 1
        WHERE parent_id IS NOT DISTINCT FROM v_target_parent
          AND "position" > v_target_position;
        
        -- When moving to a different parent, we use the target position + 1
        v_new_position := v_target_position + 1;  
    END IF;

    -- Update the moving tag with the new parent and new position
    UPDATE tags
    SET parent_id = v_target_parent,
        "position" = v_new_position
    WHERE id = p_tag_id;

    -- Return the updated tag record with all its columns
    RETURN QUERY
        SELECT id, uuid, name, description, parent_id, "position", created_at, updated_at
        FROM tags
        WHERE id = p_tag_id;
END;
$$ LANGUAGE plpgsql;

/**
 * Function: merge_tags
 * 
 * Merges two tags by transferring all children from the merging tag to the surviving tag,
 * then deletes the merging tag.
 *
 * Parameters:
 *   @surviving_tag_id - The ID of the tag that will remain after merging
 *   @merging_tag_id - The ID of the tag that will be deleted after merging
 *
 * Returns: 
 *   All columns of the surviving tag record with updated information
 *
 * Transaction Safety:
 *   Uses advisory locks on both tags to prevent concurrent modifications
 *
 * Exceptions:
 *   - Raises an exception if either tag does not exist
 *   - Raises an exception if attempting to merge a tag with itself
 *
 * Notes:
 *   - Automatically recalculates positions for all children of the surviving tag
 *   - Explicitly transfers all image associations from the merging tag to the surviving tag
 *   - Both hierarchical relationships (parent-child) and image associations are preserved
 */
CREATE OR REPLACE FUNCTION merge_tags(
    surviving_tag_id INTEGER,
    merging_tag_id INTEGER
)
RETURNS TABLE(
    id INTEGER,
    uuid UUID,
    name TEXT,
    description TEXT,
    parent_id INTEGER,
    "position" INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) AS $$
DECLARE
    v_surviving_tag RECORD; -- Record to store the validated surviving tag details
BEGIN
    -- Prevent merging a tag with itself (invalid operation)
    IF surviving_tag_id = merging_tag_id THEN
        RAISE EXCEPTION 'Cannot merge a tag with itself';
    END IF;
    
    -- Acquire advisory locks on both tags to prevent concurrent modifications
    PERFORM pg_advisory_xact_lock(surviving_tag_id);
    PERFORM pg_advisory_xact_lock(merging_tag_id);

    -- Validate the surviving tag exists and lock its row for update
    SELECT * INTO _surviving_tag 
    FROM tags 
    WHERE id = surviving_tag_id 
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Surviving tag with id % not found', surviving_tag_id;
    END IF;
    
    -- Validate the merging tag exists and lock its row for update
    PERFORM 1 FROM tags WHERE id = merging_tag_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Merging tag with id % not found', merging_tag_id;
    END IF;

    -- Reparent children of the merging tag to the surviving tag
    -- This maintains the hierarchical relationship
    UPDATE tags
    SET parent_id = surviving_tag_id
    WHERE parent_id = merging_tag_id;

    -- Reassign image_tags associations from the merging tag to the surviving tag
    -- This preserves all image-tag relationships
    UPDATE image_tags
    SET tag_id = surviving_tag_id
    WHERE tag_id = merging_tag_id;

    -- Recalculate positions for all children of the surviving tag
    -- This ensures sequential ordering (0, 1, 2...) after the merge
    -- Using ROW_NUMBER() to create a continuous sequence based on existing ordering
    WITH ordered AS (
        SELECT id,
               ROW_NUMBER() OVER (ORDER BY "position", created_at) - 1 AS new_position
        FROM tags
        WHERE parent_id = surviving_tag_id
    )
    UPDATE tags t
    SET "position" = o.new_position
    FROM ordered o
    WHERE t.id = o.id;

    -- Delete the merging tag (now safe as all associations have been transferred)
    DELETE FROM tags
    WHERE id = merging_tag_id;

    -- Return the final state of the surviving tag
    RETURN QUERY
    SELECT id, uuid, name, description, parent_id, "position", created_at, updated_at
    FROM tags
    WHERE id = surviving_tag_id;
END;
$$ LANGUAGE plpgsql;

/**
 * Function: delete_tag_recursive
 * 
 * Recursively deletes a tag and all its descendants in the tag hierarchy.
 *
 * Parameters:
 *   @p_tag_id - The ID of the root tag to delete along with its entire subtree
 *
 * Returns: 
 *   VOID - This function performs deletion operations but doesn't return any data
 *
 * Transaction Safety:
 *   Should be called within a transaction to ensure atomicity
 *
 * Performance Notes:
 *   - Uses a recursive CTE which efficiently traverses the tag hierarchy in a single query
 *   - Handles deletion in a consistent order: first image associations, then tags
 * 
 * Behavior Notes:
 *   - Safely handles tag hierarchies of any depth through recursive traversal
 *   - Automatically cleans up all image_tags associations for the deleted tags
 *   - No-op if the specified tag doesn't exist
 */
CREATE OR REPLACE FUNCTION delete_tag_recursive(
    p_tag_id INTEGER
)
RETURNS VOID AS $$
BEGIN
    -- Recursively identify the entire subtree of tags to be deleted
    WITH RECURSIVE subtree AS (
        -- Base case: the specified tag
        SELECT id
        FROM tags
        WHERE id = p_tag_id

        UNION ALL

        -- Recursive case: all children of tags already in the subtree
        SELECT t.id
        FROM tags t
        INNER JOIN subtree s ON t.parent_id = s.id
    )
    
    -- First, delete all image-tag associations for tags in the subtree
    -- This prevents foreign key violations when deleting the tags
    DELETE FROM image_tags
    WHERE tag_id IN (SELECT id FROM subtree);

    -- Then, delete all tags in the subtree (root tag and all descendants)
    DELETE FROM tags
    WHERE id IN (SELECT id FROM subtree);

    -- Function completes silently whether tags were found and deleted or not
END;
$$ LANGUAGE plpgsql;