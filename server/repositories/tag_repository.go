package repositories

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Hierarchy represents the minimal cached structure.
type Hierarchy struct {
	RootIDs  []int          `json:"rootIDs"`
	Children map[int][]int  `json:"children"`
	Mapping  map[int]string `json:"mapping"` // internal tag ID -> UUID
}

// HierarchyNode holds minimal tag info for building the hierarchy.
type HierarchyNode struct {
	ID       int
	UUID     string
	Position int
}

type TagRepository struct {
	container *container.Container
}

func NewTagRepository(container *container.Container) *TagRepository {
	return &TagRepository{
		container: container,
	}
}

func (r *TagRepository) reindexElastic(ctx context.Context, tag *models.Tag) error {
	// Construct the document to index
	document := map[string]any{
		"id":          tag.ID,
		"uuid":        tag.UUID,
		"name":        tag.Name,
		"description": tag.Description,
		"created_at":  tag.CreatedAt,
		"updated_at":  tag.UpdatedAt,
	}

	// Encode the document
	payload, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("error encoding document: %w", err)
	}

	// Create index request
	req := esapi.IndexRequest{
		Index:      "tags",
		DocumentID: tag.UUID,
		Body:       bytes.NewReader(payload),
		// Make the document immediately searchable
		Refresh: "true",
	}

	// Execute the request
	res, err := req.Do(ctx, r.container.Elastic.Client)
	if err != nil {
		return fmt.Errorf("error executing index request: %w", err)
	}

	// Handle potential close error
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			log.Error().Err(err).Msg("Failed to close Elasticsearch response body")
		}
	}()

	// Check if the request was successful
	if res.IsError() {
		var e map[string]any
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return fmt.Errorf("error parsing the response body: %w", err)
		}
		return fmt.Errorf("error indexing document [status:%s]: %v", res.Status(), e)
	}

	return nil
}

func (r *TagRepository) Reindex(ctx context.Context, tag *models.Tag) error {
	if err := r.reindexElastic(ctx, tag); err != nil {
		return fmt.Errorf("error indexing tag in Elastic: %w", err)
	}

	return nil
}

func (r *TagRepository) ReindexAll(ctx context.Context) error {
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	// Get all tag IDs
	rows, err := tx.Query(ctx, "SELECT id FROM tags ORDER BY id")
	if err != nil {
		return fmt.Errorf("error querying tag IDs: %w", err)
	}
	defer rows.Close()

	var tagIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("error scanning tag ID: %w", err)
		}
		tagIDs = append(tagIDs, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating tag IDs: %w", err)
	}

	// Commit the transaction to release the connection
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction to get IDs: %w", err)
	}

	// Iterate through IDs and reindex each tag
	for _, id := range tagIDs {
		// Get the tag by ID
		tag, err := r.GetByInternalID(ctx, id)
		if err != nil {
			// Log the error and continue to the next tag
			log.Error().Err(err).Msgf("Error retrieving tag for id %d", id)
			continue
		}

		// Reindex in a new transaction
		if err := r.Reindex(ctx, tag); err != nil {
			log.Error().Err(err).Msgf("Error reindexing tag %s", tag.UUID)
			continue
		}

		log.Info().Msgf("Reindexed tag %s", tag.UUID)
	}

	return nil
}

func (r *TagRepository) fetchAllHierarchyNodes(ctx context.Context) ([]HierarchyNode, error) {
	const query = `
        SELECT id, uuid, position
        FROM tags
        ORDER BY id
    `
	rows, err := r.container.Database.Pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []HierarchyNode
	for rows.Next() {
		var n HierarchyNode
		if err := rows.Scan(&n.ID, &n.UUID, &n.Position); err != nil {
			return nil, err
		}
		results = append(results, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *TagRepository) fetchParentToChildren(ctx context.Context) (map[int][]int, error) {
	const query = `
        SELECT ancestor, descendant
        FROM tag_closure
        WHERE depth = 1
        ORDER BY ancestor, descendant
    `
	rows, err := r.container.Database.Pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	parentToChildren := make(map[int][]int)
	for rows.Next() {
		var parent, child int
		if err := rows.Scan(&parent, &child); err != nil {
			return nil, err
		}
		parentToChildren[parent] = append(parentToChildren[parent], child)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return parentToChildren, nil
}

func (r *TagRepository) buildHierarchyFromDB(ctx context.Context) (*Hierarchy, error) {
	// Fetch all tags
	tagList, err := r.fetchAllHierarchyNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetchAllTags failed: %w", err)
	}

	// Build a quick map for sorting children.
	positionMap := make(map[int]int, len(tagList))
	// Build the mapping from internal ID to UUID.
	mapping := make(map[int]string, len(tagList))
	for _, t := range tagList {
		positionMap[t.ID] = t.Position
		mapping[t.ID] = t.UUID
	}

	// Fetch direct parent-child relationships from closure (depth=1)
	parentToChildren, err := r.fetchParentToChildren(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetchParentToChildren failed: %w", err)
	}

	// Sort each parent's children by position.
	for parent, kids := range parentToChildren {
		sort.Slice(kids, func(i, j int) bool {
			return positionMap[kids[i]] < positionMap[kids[j]]
		})
		parentToChildren[parent] = kids
	}

	// Determine root IDs: any tag that is not a descendant (child) is a root.
	childSet := make(map[int]struct{})
	for _, kids := range parentToChildren {
		for _, k := range kids {
			childSet[k] = struct{}{}
		}
	}

	var rootIDs []int
	for _, t := range tagList {
		if _, ok := childSet[t.ID]; !ok {
			rootIDs = append(rootIDs, t.ID)
		}
	}

	// Sort roots by position.
	sort.Slice(rootIDs, func(i, j int) bool {
		return positionMap[rootIDs[i]] < positionMap[rootIDs[j]]
	})

	hierarchy := &Hierarchy{
		RootIDs:  rootIDs,
		Children: parentToChildren,
		Mapping:  mapping,
	}

	return hierarchy, nil
}

func (r *TagRepository) saveHierarchyToCache(ctx context.Context, h *Hierarchy) error {
	data, err := json.Marshal(h)
	if err != nil {
		return fmt.Errorf("json marshal failed: %w", err)
	}

	err = r.container.Cache.Client.Set(ctx, "tags_hierarchy", data, 0).Err()
	if err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}

	return nil
}

func (r *TagRepository) fetchCachedHierarchy(ctx context.Context) (*Hierarchy, error) {
	data, err := r.container.Cache.Client.Get(ctx, "tags_hierarchy").Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			h, err := r.buildHierarchyFromDB(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to build hierarchy from database: %w", err)
			}

			if err := r.saveHierarchyToCache(ctx, h); err != nil {
				log.Error().Err(err).Msg("Failed to cache hierarchy")
			}

			return h, nil
		}
		return nil, fmt.Errorf("redis get failed: %w", err)
	}

	var h Hierarchy
	if err := json.Unmarshal(data, &h); err != nil {
		return nil, fmt.Errorf("json unmarshal failed: %w", err)
	}
	return &h, nil
}

func (r *TagRepository) filterHierarchy(h *Hierarchy, filterIDs []int) *Hierarchy {
	if h == nil {
		return &Hierarchy{
			RootIDs:  []int{},
			Children: map[int][]int{},
			Mapping:  map[int]string{},
		}
	}

	// Build a parent map (child -> parent).
	childToParent := make(map[int]int)
	for parent, children := range h.Children {
		for _, c := range children {
			childToParent[c] = parent
		}
	}

	// Collect all IDs needed: each matching tag plus all its ancestors.
	needed := make(map[int]bool)
	for _, id := range filterIDs {
		curr := id
		for {
			needed[curr] = true
			parent, ok := childToParent[curr]
			if !ok {
				break // reached a root
			}
			curr = parent
		}
	}

	// Filter root IDs to only those that are needed.
	var newRootIDs []int
	for _, rID := range h.RootIDs {
		if needed[rID] {
			newRootIDs = append(newRootIDs, rID)
		}
	}

	// Build new children map including only needed nodes.
	newChildren := make(map[int][]int)
	for parent, kids := range h.Children {
		if !needed[parent] {
			continue
		}
		var filteredKids []int
		for _, c := range kids {
			if needed[c] {
				filteredKids = append(filteredKids, c)
			}
		}
		newChildren[parent] = filteredKids
	}

	return &Hierarchy{
		RootIDs:  newRootIDs,
		Children: newChildren,
		Mapping:  h.Mapping,
	}
}

func (r *TagRepository) deleteHierarchyFromCache(ctx context.Context) error {
	_, err := r.container.Cache.Client.Del(ctx, "tags_hierarchy").Result()
	if err != nil {
		return fmt.Errorf("redis del failed: %w", err)
	}
	return nil
}

func (r *TagRepository) getByInternalIDTx(ctx context.Context, tx pgx.Tx, id int64) (*models.Tag, error) {
	query := `
		SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
		FROM tags
		WHERE id = $1
	`

	var tag models.Tag
	var parentIdPtr *int64

	err := tx.QueryRow(ctx, query, id).Scan(
		&tag.ID, &tag.UUID, &tag.Name, &tag.Description, &parentIdPtr, &tag.Position, &tag.CreatedAt, &tag.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrTagNotFound
		}
		return nil, fmt.Errorf("error fetching tag: %w", err)
	}

	tag.ParentID = parentIdPtr

	return &tag, nil
}

// Returns a single tag using its internal ID.
func (r *TagRepository) GetByInternalID(ctx context.Context, id int64) (*models.Tag, error) {
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	// Ensure we handle rollback errors
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				log.Error().Err(err).Msg("Failed to roll back transaction")
			}
		}
	}()

	tag, err := r.getByInternalIDTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return tag, nil

}

func (r *TagRepository) getByUUIDTx(ctx context.Context, tx pgx.Tx, uuid string) (*models.Tag, error) {
	query := `
		SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
		FROM tags
		WHERE uuid = $1
	`

	var tag models.Tag
	var parentIdPtr *int64

	err := tx.QueryRow(ctx, query, uuid).Scan(
		&tag.ID, &tag.UUID, &tag.Name, &tag.Description, &parentIdPtr, &tag.Position, &tag.CreatedAt, &tag.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrTagNotFound
		}
		return nil, fmt.Errorf("error fetching tag: %w", err)
	}

	tag.ParentID = parentIdPtr

	return &tag, nil
}

// Returns a single tag using its UUID.
func (r *TagRepository) GetByUUID(ctx context.Context, uuid string) (*models.Tag, error) {
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	// Ensure we handle rollback errors
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				log.Error().Err(err).Msg("Failed to roll back transaction")
			}
		}
	}()

	tag, err := r.getByUUIDTx(ctx, tx, uuid)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return tag, nil
}

// Deletes a single tag and all of its descendants from the database.
func (r *TagRepository) Delete(ctx context.Context, tagUUID string) error {

	// Start a transaction.
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(rollbackErr).Msg("Failed to rollback transaction")
			}
		}
	}()

	// Defer constraint.
	_, err = tx.Exec(ctx, "SET CONSTRAINTS tags_unique_parent_id_position DEFERRED")
	if err != nil {
		return fmt.Errorf("error deferring constraints: %w", err)
	}

	// Fetch the tag to be deleted.
	tag, err := r.getByUUIDTx(ctx, tx, tagUUID)
	if err != nil {
		return fmt.Errorf("error fetching tag: %w", err)
	}

	// Compute the set of tags to be deleted: the tag itself and all its descendants.
	descendantUUIDs, err := r.fetchAllDescendantTagUUIDsTx(ctx, tx, tag.ID)
	if err != nil {
		return fmt.Errorf("error fetching descendant tags: %w", err)
	}

	// Build a set (map) of tag UUIDs.
	tagsToDelete := make(map[string]struct{})
	tagsToDelete[tag.UUID] = struct{}{}
	for _, dUUID := range descendantUUIDs {
		tagsToDelete[dUUID] = struct{}{}
	}

	// Find all affected images (those referencing any of the tags in tagsToDelete).
	affectedImageUUIDs, err := r.fetchImagesReferencingTagsTx(ctx, tx, tagsToDelete)
	if err != nil {
		return fmt.Errorf("error fetching affected images: %w", err)
	}

	// We can use the closure table to get all tags in the subtree.
	// This query deletes all tags whose id is in the subtree rooted at the current tag.
	_, err = tx.Exec(ctx, `
        DELETE FROM tags WHERE id IN (
			SELECT descendant FROM tag_closure WHERE ancestor = $1
        )
    `, tag.ID)
	if err != nil {
		return fmt.Errorf("error deleting tags: %w", err)
	}

	// Commit the transaction.
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	// Delete each removed tag from Elasticsearch.
	// (If tags are stored in a separate ES index, do a delete request for each.)
	for delUUID := range tagsToDelete {
		req := esapi.DeleteRequest{
			Index:      "tags",
			DocumentID: delUUID,
			Refresh:    "true",
		}

		res, err := req.Do(ctx, r.container.Elastic.Client)
		if err != nil {
			log.Error().Err(err).Str("tagUUID", delUUID).Msg("Failed to delete tag from Elasticsearch")
			continue
		}

		defer func() {
			if closeErr := res.Body.Close(); closeErr != nil {
				log.Error().Err(closeErr).Msg("Failed to close Elasticsearch response body")
			}
		}()

		if res.IsError() {
			if res.StatusCode != 404 {
				var e map[string]any
				if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
					log.Error().Err(err).Msg("Failed to parse Elasticsearch error response")
				} else {
					log.Error().Err(err).Str("status", res.Status()).Msg("Failed to delete document from Elasticsearch index")
				}
			}
		}
	}

	// Enqueue reindexing jobs for affected images.
	for _, imageUUID := range affectedImageUUIDs {
		if err := r.container.Worker.EnqueueReindexImage(ctx, imageUUID); err != nil {
			log.Error().Err(err).Str("imageUUID", imageUUID).Msg("Failed to enqueue image reindex")
		}
	}

	// Invalidate the hierarchy cache.
	if err := r.deleteHierarchyFromCache(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to invalidate tag hierarchy cache")
	}

	return nil
}

// Creates a new tag at either the root level or as a child of an existing parent, with sibling ordering.
func (r *TagRepository) Create(ctx context.Context, name string, description string, parentUuid *string, beforeUuid *string, afterUuid *string) (*models.Tag, error) {
	// Start a transaction.
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	// Rollback if something goes wrong.
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				log.Error().Err(err).Msg("Failed to roll back transaction")
			}
		}
	}()

	// Defer the uniqueness check until commit.
	_, err = tx.Exec(ctx, "SET CONSTRAINTS tags_unique_parent_id_position DEFERRED")
	if err != nil {
		return nil, fmt.Errorf("error deferring constraint check: %w", err)
	}

	// Ensure the name is provided and unique.
	if name == "" {
		return nil, fmt.Errorf("name must be provided")
	}
	exists, err := r.tagNameExistsTx(ctx, tx, name, nil)
	if err != nil {
		return nil, fmt.Errorf("error checking tag name uniqueness: %w", err)
	}
	if exists {
		return nil, fmt.Errorf("tag name %s already exists", name)
	}

	// Determine parent_id.
	var parentID *int64
	if parentUuid != nil {
		parentTag, err := r.getByUUIDTx(ctx, tx, *parentUuid)
		if err != nil {
			return nil, fmt.Errorf("parent tag not found: %w", err)
		}
		parentID = &parentTag.ID
	}

	// Validate sibling ordering.
	if beforeUuid != nil || afterUuid != nil {
		// If parent is not provided, derive from the sibling.
		if parentUuid == nil {
			var sibling *models.Tag
			if beforeUuid != nil {
				sibling, err = r.getByUUIDTx(ctx, tx, *beforeUuid)
			} else {
				sibling, err = r.getByUUIDTx(ctx, tx, *afterUuid)
			}
			if err != nil {
				return nil, fmt.Errorf("failed to fetch sibling tag: %w", err)
			}
			parentID = sibling.ParentID // May be nil for a root-level sibling.
		}
	}

	// Compute the new tag's position.
	newPosition, err := r.computeSortOrder(ctx, tx, beforeUuid, afterUuid, parentID)
	if err != nil {
		return nil, fmt.Errorf("error computing new sort order: %w", err)
	}

	// Insert the new tag and generate a public UUID.
	var newTagID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO tags (name, description, parent_id, position)
		VALUES ($1, $2, $3, $4)
		RETURNING id
    `, name, description, parentID, newPosition).Scan(&newTagID)
	if err != nil {
		return nil, fmt.Errorf("error inserting new tag: %w", err)
	}

	// Rebuild the closure table for the new tag's subtree.
	// For a new tag, its subtree is itself.
	if err := r.rebuildClosureForSubtreeTx(ctx, tx, newTagID); err != nil {
		return nil, fmt.Errorf("error rebuilding closure for new tag: %w", err)
	}

	// Commit the transaction.
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	// Return Updated Tag
	newTag, err := r.GetByInternalID(ctx, newTagID)
	if err != nil {
		return nil, fmt.Errorf("error fetching created tag: %w", err)
	}

	// Queue a reindex job
	if err := r.container.Worker.EnqueueReindexTag(ctx, newTag.UUID); err != nil {
		log.Error().Err(err).Str("tagUUID", newTag.UUID).Msg("Failed to enqueue tag reindex")
	}

	// Invalidate the hierarchy cache.
	if err := r.deleteHierarchyFromCache(ctx); err != nil {
		log.Error().Err(err).Msg("failed to invalidate tag hierarchy cache")
	}

	return newTag, nil
}

func (r *TagRepository) Update(ctx context.Context, tagUUID string, newName string, newDescription string, newParentUUID *string, beforeUUID *string, afterUUID *string) (*models.Tag, error) {
	// Start a transaction.
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	// Rollback if something goes wrong.
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				log.Error().Err(err).Msg("Failed to roll back transaction")
			}
		}
	}()

	// Defer the uniqueness check until commit.
	_, err = tx.Exec(ctx, "SET CONSTRAINTS tags_unique_parent_id_position DEFERRED")
	if err != nil {
		return nil, fmt.Errorf("error deferring constraint check: %w", err)
	}

	// Fetch current tag.
	currentTag, err := r.getByUUIDTx(ctx, tx, tagUUID)
	if err != nil {
		return nil, fmt.Errorf("error fetching tag: %w", err)
	}

	// Validate uniqueness of new name if it is being changed.
	if newName != "" && newName != currentTag.Name {
		exists, err := r.tagNameExistsTx(ctx, tx, newName, &currentTag.UUID)
		if err != nil {
			return nil, fmt.Errorf("error validating tag name uniqueness: %w", err)
		}
		if exists {
			return nil, fmt.Errorf("tag name %s already exists", newName)
		}
	}

	// Determine which changes are needed.
	renameChange := newName != "" && newName != currentTag.Name
	descriptionChange := newDescription != "" && newDescription != currentTag.Description
	moveChange := false
	reorderChange := false

	// newParentID holds the new parent's internal ID (nil for root-level).
	var newParentID *int64
	if newParentUUID != nil {
		newParentTag, err := r.getByUUIDTx(ctx, tx, *newParentUUID)
		if err != nil {
			return nil, fmt.Errorf("new parent tag not found: %w", err)
		}
		// Check if the new parent's ID differs from the current one.
		if currentTag.ParentID == nil || newParentTag.ID != *currentTag.ParentID {
			moveChange = true
		}
		newParentID = &newParentTag.ID
	} else {
		// If no parent is explicitly provided, preserve the current parent.
		newParentID = currentTag.ParentID
	}

	// Validate sibling parameters.
	if beforeUUID != nil || afterUUID != nil {
		reorderChange = true
		// If no explicit parent is given, derive newParentID from the sibling.
		if newParentUUID == nil {
			var sibling *models.Tag
			if beforeUUID != nil {
				sibling, err = r.getByUUIDTx(ctx, tx, *beforeUUID)
			} else {
				sibling, err = r.getByUUIDTx(ctx, tx, *afterUUID)
			}
			if err != nil {
				return nil, fmt.Errorf("failed to fetch sibling tag: %w", err)
			}
			// If the sibling's parent differs from the current tag's parent, mark it as a move.
			if sibling.ParentID == nil {
				if currentTag.ParentID != nil {
					moveChange = true
				}
				newParentID = nil
			} else {
				if currentTag.ParentID == nil || *sibling.ParentID != *currentTag.ParentID {
					moveChange = true
				}
				newParentID = sibling.ParentID
			}
		}
	}

	// Update metadata if needed.
	if renameChange || descriptionChange {
		_, err = tx.Exec(ctx,
			`UPDATE tags SET name = $1, description = $2 WHERE id = $3`,
			func() string {
				if renameChange {
					return newName
				}
				return currentTag.Name
			}(),
			func() string {
				if descriptionChange {
					return newDescription
				}
				return currentTag.Description
			}(),
			currentTag.ID,
		)
		if err != nil {
			return nil, fmt.Errorf("error updating tag metadata: %w", err)
		}
	}

	// If a move is requested, update the parent and rebuild the closure table.
	if moveChange {
		// Prevent cycles: ensure new parent is not a descendant of the current tag.
		isDesc, err := r.isDescendantTx(ctx, tx, newParentID, currentTag.ID)
		if err != nil {
			return nil, fmt.Errorf("error checking tag ancestry: %w", err)
		}
		if isDesc {
			return nil, fmt.Errorf("cannot move tag into one of its descendants")
		}

		// Update the tag's parent.
		_, err = tx.Exec(ctx,
			`UPDATE tags SET parent_id = $1 WHERE id = $2`,
			newParentID, currentTag.ID,
		)
		if err != nil {
			return nil, fmt.Errorf("error updating tag parent: %w", err)
		}
	}

	// Update position if reordering is requested.
	var newPosition int64
	if reorderChange {
		newPosition, err = r.computeSortOrder(ctx, tx, beforeUUID, afterUUID, newParentID)
		if err != nil {
			return nil, fmt.Errorf("error computing new sort order: %w", err)
		}
		// Update the tag's position.
		_, err = tx.Exec(ctx,
			`UPDATE tags SET position = $1 WHERE id = $2`,
			newPosition, currentTag.ID,
		)
		if err != nil {
			return nil, fmt.Errorf("error updating tag position: %w", err)
		}
	} else if moveChange && !reorderChange {
		// If moving without explicit sibling ordering, place the tag at the end.
		newPosition, err = r.computeSortOrder(ctx, tx, nil, nil, newParentID)
		if err != nil {
			return nil, fmt.Errorf("error computing new sort order at end: %w", err)
		}
		_, err = tx.Exec(ctx,
			`UPDATE tags SET position = $1, updated_at = NOW() WHERE id = $2`,
			newPosition, currentTag.ID,
		)
		if err != nil {
			return nil, fmt.Errorf("error updating tag position: %w", err)
		}
	}

	// Rebuild the closure table for the affected subtree if the tag has moved.
	if moveChange {
		// Rebuild the closure for the subtree rooted at the current tag.
		// For a top-level tag, currentTag.ID is the root.
		if err := r.rebuildClosureForSubtreeTx(ctx, tx, currentTag.ID); err != nil {
			return nil, fmt.Errorf("error rebuilding closure for tag subtree: %w", err)
		}
	}

	// Compute affected images for reindexing.
	affectedTagUUIDs := make(map[string]struct{})
	if moveChange {
		descendantUUIDs, err := r.fetchAllDescendantTagUUIDsTx(ctx, tx, currentTag.ID)
		if err != nil {
			return nil, fmt.Errorf("error fetching descendant tags: %w", err)
		}
		for _, uuid := range descendantUUIDs {
			affectedTagUUIDs[uuid] = struct{}{}
		}
		affectedTagUUIDs[currentTag.UUID] = struct{}{}
	} else if renameChange || descriptionChange {
		affectedTagUUIDs[currentTag.UUID] = struct{}{}
	}
	var affectedImageUUIDs []string
	if len(affectedTagUUIDs) > 0 {
		affectedImageUUIDs, err = r.fetchImagesReferencingTagsTx(ctx, tx, affectedTagUUIDs)
		if err != nil {
			return nil, fmt.Errorf("error fetching affected images: %w", err)
		}
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	// Enqueue reindexing jobs for affected images.
	for _, imageUUID := range affectedImageUUIDs {
		if err := r.container.Worker.EnqueueReindexImage(ctx, imageUUID); err != nil {
			log.Error().Err(err).Str("imageUUID", imageUUID).Msg("Failed to enqueue image reindex")
		}
	}

	// Invalidate the hierarchy cache.
	if err := r.deleteHierarchyFromCache(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to invalidate tag hierarchy cache")
	}

	// Return the updated tag.
	updatedTag, err := r.GetByUUID(ctx, currentTag.UUID)
	if err != nil {
		return nil, fmt.Errorf("error fetching updated tag: %w", err)
	}

	return updatedTag, nil
}

func (r *TagRepository) computeSortOrder(ctx context.Context, tx pgx.Tx, beforeUUID *string, afterUUID *string, parentID *int64) (int64, error) {
	// Fetch siblings for the given parent.
	siblings, err := r.fetchChildrenByParentTx(ctx, tx, parentID)
	if err != nil {
		return 0, fmt.Errorf("error fetching siblings: %w", err)
	}

	// Both before and after provided.
	if beforeUUID != nil && afterUUID != nil {
		beforeTag, err := r.getByUUIDTx(ctx, tx, *beforeUUID)
		if err != nil {
			return 0, fmt.Errorf("error fetching beforeTag: %w", err)
		}
		afterTag, err := r.getByUUIDTx(ctx, tx, *afterUUID)
		if err != nil {
			return 0, fmt.Errorf("error fetching afterTag: %w", err)
		}
		gap := afterTag.Position - beforeTag.Position
		if gap > 1 {
			return beforeTag.Position + gap/2, nil
		}
		// Renormalize siblings if no gap available.
		if err := r.renormalizeSortOrders(ctx, tx, siblings); err != nil {
			return 0, fmt.Errorf("error renormalizing sort orders: %w", err)
		}
		// Re-fetch updated beforeTag and afterTag.
		beforeTag, err = r.getByUUIDTx(ctx, tx, *beforeUUID)
		if err != nil {
			return 0, fmt.Errorf("error re-fetching beforeTag: %w", err)
		}
		afterTag, err = r.getByUUIDTx(ctx, tx, *afterUUID)
		if err != nil {
			return 0, fmt.Errorf("error re-fetching afterTag: %w", err)
		}
		gap = afterTag.Position - beforeTag.Position
		return beforeTag.Position + gap/2, nil
	} else if beforeUUID != nil {
		beforeTag, err := r.getByUUIDTx(ctx, tx, *beforeUUID)
		if err != nil {
			return 0, fmt.Errorf("error fetching beforeTag: %w", err)
		}
		newPos := beforeTag.Position - 1
		if !r.isValidSortOrder(ctx, tx, parentID, newPos) {
			if err := r.renormalizeSortOrders(ctx, tx, siblings); err != nil {
				return 0, fmt.Errorf("error renormalizing sort orders: %w", err)
			}
			beforeTag, err = r.getByUUIDTx(ctx, tx, *beforeUUID)
			if err != nil {
				return 0, fmt.Errorf("error re-fetching beforeTag: %w", err)
			}
			newPos = beforeTag.Position - 1
		}
		return newPos, nil
	} else if afterUUID != nil {
		afterTag, err := r.getByUUIDTx(ctx, tx, *afterUUID)
		if err != nil {
			return 0, fmt.Errorf("error fetching afterTag: %w", err)
		}
		newPos := afterTag.Position + 1
		if !r.isValidSortOrder(ctx, tx, parentID, newPos) {
			if err := r.renormalizeSortOrders(ctx, tx, siblings); err != nil {
				return 0, fmt.Errorf("error renormalizing sort orders: %w", err)
			}
			afterTag, err = r.getByUUIDTx(ctx, tx, *afterUUID)
			if err != nil {
				return 0, fmt.Errorf("error re-fetching afterTag: %w", err)
			}
			newPos = afterTag.Position + 1
		}
		return newPos, nil
	} else {
		// No positioning provided, place at end.
		if len(siblings) == 0 {
			return 10, nil
		}
		lastSibling := siblings[len(siblings)-1]
		return lastSibling.Position + 10, nil
	}
}

func (r *TagRepository) renormalizeSortOrders(ctx context.Context, tx pgx.Tx, siblings []models.Tag) error {
	const gap int64 = 10
	newOrder := gap
	// Ensure siblings are sorted by current position.
	sort.Slice(siblings, func(i, j int) bool {
		return siblings[i].Position < siblings[j].Position
	})
	for _, tag := range siblings {
		_, err := tx.Exec(ctx,
			`UPDATE tags SET position = $1 WHERE id = $2`,
			newOrder, tag.ID,
		)
		if err != nil {
			return fmt.Errorf("error updating sort order for tag %d: %w", tag.ID, err)
		}
		newOrder += gap
	}
	return nil
}

func (r *TagRepository) isValidSortOrder(ctx context.Context, tx pgx.Tx, parentID *int64, pos int64) bool {
	var count int
	err := tx.QueryRow(ctx,
		`SELECT COUNT(1) FROM tags WHERE parent_id = $1 AND position = $2`,
		parentID, pos,
	).Scan(&count)
	if err != nil {
		log.Error().Err(err).Msg("error validating sort order")
		return false
	}
	return count == 0
}

func (r *TagRepository) fetchChildrenByParentTx(ctx context.Context, tx pgx.Tx, parentID *int64) ([]models.Tag, error) {
	var rows pgx.Rows
	var err error

	if parentID == nil {
		rows, err = tx.Query(ctx, `
            SELECT id, uuid, name, description, parent_id, position, created_at, updated_at 
            FROM tags 
            WHERE parent_id IS NULL 
            ORDER BY position ASC
        `)
	} else {
		rows, err = tx.Query(ctx, `
            SELECT id, uuid, name, description, parent_id, position, created_at, updated_at 
            FROM tags 
            WHERE parent_id = $1 
            ORDER BY position ASC
        `, *parentID)
	}
	if err != nil {
		return nil, fmt.Errorf("error fetching children: %w", err)
	}
	defer rows.Close()

	var tags []models.Tag
	for rows.Next() {
		var tag models.Tag
		if err := rows.Scan(&tag.ID, &tag.UUID, &tag.Name, &tag.Description, &tag.ParentID, &tag.Position, &tag.CreatedAt, &tag.UpdatedAt); err != nil {
			return nil, fmt.Errorf("error scanning child tag: %w", err)
		}
		tags = append(tags, tag)
	}
	return tags, nil
}

func (r *TagRepository) fetchAllDescendantTagUUIDsTx(ctx context.Context, tx pgx.Tx, tagID int64) ([]string, error) {
	rows, err := tx.Query(ctx, `
         SELECT t.uuid
         FROM tag_closure tc 
         JOIN tags t ON tc.descendant = t.id 
         WHERE tc.ancestor = $1 AND tc.depth > 0
    `, tagID)
	if err != nil {
		return nil, fmt.Errorf("error querying descendant tags: %w", err)
	}
	defer rows.Close()

	var uuids []string
	for rows.Next() {
		var uuid string
		if err := rows.Scan(&uuid); err != nil {
			return nil, fmt.Errorf("error scanning descendant uuid: %w", err)
		}
		uuids = append(uuids, uuid)
	}
	return uuids, nil
}

func (r *TagRepository) fetchImagesReferencingTagsTx(ctx context.Context, tx pgx.Tx, tagUUIDs map[string]struct{}) ([]string, error) {
	// Convert the map keys to a slice.
	uuids := make([]string, 0, len(tagUUIDs))
	for uuid := range tagUUIDs {
		uuids = append(uuids, uuid)
	}

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT i.uuid 
		FROM image_tags it
		LEFT JOIN tags t ON t.id = it.tag_id
		LEFT JOIN images i ON i.id = it.image_id
		WHERE t.uuid = ANY($1)
    `, uuids)
	if err != nil {
		return nil, fmt.Errorf("error fetching images referencing tags: %w", err)
	}
	defer rows.Close()

	var imageUUIDs []string
	for rows.Next() {
		var imageUUID string
		if err := rows.Scan(&imageUUID); err != nil {
			return nil, fmt.Errorf("error scanning image uuid: %w", err)
		}
		imageUUIDs = append(imageUUIDs, imageUUID)
	}
	return imageUUIDs, nil
}

func (r *TagRepository) isDescendantTx(ctx context.Context, tx pgx.Tx, newParentID *int64, currentTagID int64) (bool, error) {
	if newParentID == nil {
		return false, nil
	}
	var count int
	err := tx.QueryRow(ctx, `
		SELECT COUNT(1) 
		FROM tag_closure 
		WHERE ancestor = $1 AND descendant = $2
    `, currentTagID, *newParentID).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("error checking descendant relationship: %w", err)
	}
	return count > 0, nil
}

func (r *TagRepository) tagNameExistsTx(ctx context.Context, tx pgx.Tx, name string, excludeUUID *string) (bool, error) {
	var count int

	if excludeUUID != nil {
		err := tx.QueryRow(ctx, `
			SELECT COUNT(1) FROM tags WHERE name = $1 AND uuid != $2
		`, name, excludeUUID).Scan(&count)
		if err != nil {
			return false, fmt.Errorf("error checking tag name uniqueness: %w", err)
		}
	} else {
		err := tx.QueryRow(ctx, `
			SELECT COUNT(1) FROM tags WHERE name = $1
		`, name).Scan(&count)
		if err != nil {
			return false, fmt.Errorf("error checking tag name uniqueness: %w", err)
		}
	}

	return count > 0, nil
}

// internalIDsToUUIDs converts a slice of internal IDs to UUID strings using the cached mapping.
func (r *TagRepository) internalIDsToUUIDs(h *Hierarchy, ids []int) ([]string, error) {
	var uuids []string
	for _, id := range ids {
		if uuid, ok := h.Mapping[id]; ok {
			uuids = append(uuids, uuid)
		} else {
			return nil, fmt.Errorf("no UUID found for internal id: %d", id)
		}
	}
	return uuids, nil
}

// getTagsByUUIDsFromES retrieves full tag details from Elasticsearch using UUIDs as keys.
func (r *TagRepository) getTagsByUUIDsFromES(ctx context.Context, uuids []string) (map[string]*models.Tag, error) {
	// Execute the mget query on the "tags" index.
	res, err := r.container.Elastic.Client.Mget().Index("tags").Ids(uuids...).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch mget failed: %w", err)
	}

	// Prepare a map to hold the tag details.
	tagMap := make(map[string]*models.Tag)
	for _, doc := range res.Docs {
		var tag models.Tag
		// Unmarshal the ES document source into the tag model.
		if err := json.Unmarshal(doc.(*types.GetResult).Source_, &tag); err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal tag from Elasticsearch")
			continue
		}
		tagMap[tag.UUID] = &tag
	}
	return tagMap, nil
}

// getUUIDFromMapping is a helper that retrieves the UUID for a given internal id from the cached hierarchy.
func (r *TagRepository) getUUIDFromMapping(h *Hierarchy, id int) (string, bool) {
	uuid, ok := h.Mapping[id]
	return uuid, ok
}

// buildTreeFromHierarchy builds the TagNode tree recursively from the hierarchy and tag metadata.
func (r *TagRepository) buildTreeFromHierarchy(h *Hierarchy, tagMap map[string]*models.Tag) []*models.TagNode {
	var buildTree func(id int) *models.TagNode
	buildTree = func(id int) *models.TagNode {
		uuid, ok := r.getUUIDFromMapping(h, id)
		if !ok {
			return nil
		}
		tag, exists := tagMap[uuid]
		if !exists {
			return nil
		}
		node := &models.TagNode{
			Tag: *tag,
		}
		for _, childID := range h.Children[id] {
			if childNode := buildTree(childID); childNode != nil {
				node.Children = append(node.Children, childNode)
			}
		}
		return node
	}

	var tree []*models.TagNode
	for _, rootID := range h.RootIDs {
		if node := buildTree(rootID); node != nil {
			tree = append(tree, node)
		}
	}
	return tree
}

// prepareSearchQuery builds a typed Elasticsearch query based on the TagFilter.
func (r *TagRepository) prepareSearchQuery(filter *models.TagFilter, size int) (*search.Request, error) {
	var mustQueries []types.Query

	if filter.Name != "" {
		mustQueries = append(mustQueries, types.Query{
			Match: map[string]types.MatchQuery{
				"name": {
					Query: filter.Name,
					Boost: utils.NewPointer(float32(2.0)),
				},
			},
		})
	}
	if filter.Description != "" {
		mustQueries = append(mustQueries, types.Query{
			Match: map[string]types.MatchQuery{
				"description": {
					Query: filter.Description,
				},
			},
		})
	}
	if filter.SinceDate != nil || filter.BeforeDate != nil {
		rangeQuery := types.DateRangeQuery{}
		if filter.SinceDate != nil {
			rangeQuery.Gte = utils.NewPointer(filter.SinceDate.Format(time.RFC3339))
		}
		if filter.BeforeDate != nil {
			rangeQuery.Lte = utils.NewPointer(filter.BeforeDate.Format(time.RFC3339))
		}
		mustQueries = append(mustQueries, types.Query{
			Range: map[string]types.RangeQuery{
				"created_at": rangeQuery,
			},
		})
	}

	// Build the search request.
	req := &search.Request{
		Size: utils.NewPointer(size + 1),
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: mustQueries,
			},
		},
	}

	return req, nil
}

// Search executes a search against Elasticsearch using the typed client
// and returns a tree of TagNodes. If no filter is provided, it returns the full tree.
func (r *TagRepository) Search(ctx context.Context, filter *models.TagFilter) ([]*models.TagNode, error) {
	// Fetch the cached hierarchy (which contains the minimal structure and mapping).
	h, err := r.fetchCachedHierarchy(ctx)
	if err != nil {
		return nil, err
	}

	// If no filter criteria are provided, simply return the full tree.
	if filter == nil || (filter.Name == "" && filter.Description == "" && filter.SinceDate == nil && filter.BeforeDate == nil) {
		// Collect all internal IDs from the hierarchy.
		idsSet := make(map[int]struct{})
		var collectIDs func(ids []int)
		collectIDs = func(ids []int) {
			for _, id := range ids {
				idsSet[id] = struct{}{}
				if children, ok := h.Children[id]; ok {
					collectIDs(children)
				}
			}
		}
		collectIDs(h.RootIDs)

		var allInternalIDs []int
		for id := range idsSet {
			allInternalIDs = append(allInternalIDs, id)
		}

		// Convert internal IDs to UUIDs.
		uuids, err := r.internalIDsToUUIDs(h, allInternalIDs)
		if err != nil {
			return nil, fmt.Errorf("failed to map internal IDs to UUIDs: %w", err)
		}

		// Retrieve full tag details from ES via a bulk mget.
		tagMap, err := r.getTagsByUUIDsFromES(ctx, uuids)
		if err != nil {
			return nil, fmt.Errorf("failed to get tags from elasticsearch: %w", err)
		}

		// Build and return the complete tag tree.
		return r.buildTreeFromHierarchy(h, tagMap), nil
	}

	// For filtering, calculate the total number of tags from the hierarchy mapping.
	totalTags := len(h.Mapping)

	// Build the Elasticsearch query using our helper.
	query, err := r.prepareSearchQuery(filter, totalTags)
	if err != nil {
		return nil, fmt.Errorf("error building search query: %w", err)
	}

	// Execute the search using the typed client.
	res, err := r.container.Elastic.Client.Search().
		Index("tags").
		Request(query).
		TrackTotalHits(true).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("error executing search: %w", err)
	}

	// Extract matching UUIDs from the ES hits.
	var matchingUUIDs []string
	for _, hit := range res.Hits.Hits {
		matchingUUIDs = append(matchingUUIDs, *hit.Id_)
	}
	if len(matchingUUIDs) == 0 {
		return []*models.TagNode{}, nil
	}

	// Build a reverse mapping (UUID → internal ID) from the cached hierarchy.
	reverseMapping := make(map[string]int)
	for internalID, uuid := range h.Mapping {
		reverseMapping[uuid] = internalID
	}

	// Convert matching UUIDs into matching internal IDs.
	var matchingInternalIDs []int
	for _, uuid := range matchingUUIDs {
		if internalID, ok := reverseMapping[uuid]; ok {
			matchingInternalIDs = append(matchingInternalIDs, internalID)
		}
	}
	if len(matchingInternalIDs) == 0 {
		return []*models.TagNode{}, nil
	}

	// Filter the cached hierarchy to include only matching tags and their ancestors.
	filteredHierarchy := r.filterHierarchy(h, matchingInternalIDs)

	// Collect all internal IDs from the filtered hierarchy.
	idsSet := make(map[int]struct{})
	var collectIDs func(ids []int)
	collectIDs = func(ids []int) {
		for _, id := range ids {
			idsSet[id] = struct{}{}
			if kids, ok := filteredHierarchy.Children[id]; ok {
				collectIDs(kids)
			}
		}
	}
	collectIDs(filteredHierarchy.RootIDs)
	var allInternalIDs []int
	for id := range idsSet {
		allInternalIDs = append(allInternalIDs, id)
	}

	// Convert these internal IDs to UUIDs.
	uuids, err := r.internalIDsToUUIDs(filteredHierarchy, allInternalIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to map internal IDs to UUIDs: %w", err)
	}

	// Retrieve full tag details from ES via a bulk mget call.
	tagMap, err := r.getTagsByUUIDsFromES(ctx, uuids)
	if err != nil {
		return nil, fmt.Errorf("failed to get tags from elasticsearch: %w", err)
	}

	// Rebuild the tag tree from the filtered hierarchy and ES metadata.
	tree := r.buildTreeFromHierarchy(filteredHierarchy, tagMap)
	return tree, nil
}

func (r *TagRepository) Merge(ctx context.Context, sourceUUID string, targetUUID string, finalName string, finalDescription string) error {
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(rollbackErr).Msg("failed to rollback transaction")
			}
		}
	}()

	// Defer unique constraint checks.
	_, err = tx.Exec(ctx, "SET CONSTRAINTS tags_unique_parent_id_position DEFERRED")
	if err != nil {
		return fmt.Errorf("error deferring constraints: %w", err)
	}

	// Fetch source and target tags.
	sourceTag, err := r.getByUUIDTx(ctx, tx, sourceUUID)
	if err != nil {
		return fmt.Errorf("source tag not found: %w", err)
	}
	targetTag, err := r.getByUUIDTx(ctx, tx, targetUUID)
	if err != nil {
		return fmt.Errorf("target tag not found: %w", err)
	}
	if sourceTag.ID == targetTag.ID {
		return fmt.Errorf("source and target tags cannot be the same")
	}

	// Ensure that the target is not a descendant of the source.
	isDesc, err := r.isDescendantTx(ctx, tx, &sourceTag.ID, targetTag.ID)
	if err != nil {
		return fmt.Errorf("error checking ancestry: %w", err)
	}
	if isDesc {
		return fmt.Errorf("cannot merge: target is a descendant of source")
	}

	// Update the target tag's metadata.
	_, err = tx.Exec(ctx,
		`UPDATE tags SET name = $1, description = $2 WHERE id = $3`,
		finalName, finalDescription, targetTag.ID,
	)
	if err != nil {
		return fmt.Errorf("error updating target tag: %w", err)
	}

	// Reparent all direct children of the source tag to the target tag.
	_, err = tx.Exec(ctx,
		`UPDATE tags SET parent_id = $1 WHERE parent_id = $2`,
		targetTag.ID, sourceTag.ID,
	)
	if err != nil {
		return fmt.Errorf("error reparenting children: %w", err)
	}

	// Rebuild the closure table for the affected subtree.
	// We rebuild the subtree rooted at the target tag so that it reflects the new structure.
	if err := r.rebuildClosureForSubtreeTx(ctx, tx, targetTag.ID); err != nil {
		return fmt.Errorf("error rebuilding closure for target subtree: %w", err)
	}

	// Determine affected images.
	affectedTagUUIDs := make(map[string]struct{})
	descendantUUIDs, err := r.fetchAllDescendantTagUUIDsTx(ctx, tx, sourceTag.ID)
	if err != nil {
		return fmt.Errorf("error fetching descendant tags: %w", err)
	}
	affectedTagUUIDs[sourceTag.UUID] = struct{}{}
	for _, uuid := range descendantUUIDs {
		affectedTagUUIDs[uuid] = struct{}{}
	}
	affectedImageUUIDs, err := r.fetchImagesReferencingTagsTx(ctx, tx, affectedTagUUIDs)
	if err != nil {
		return fmt.Errorf("error fetching affected images: %w", err)
	}

	// Delete the source tag.
	_, err = tx.Exec(ctx, `DELETE FROM tags WHERE id = $1`, sourceTag.ID)
	if err != nil {
		return fmt.Errorf("error deleting source tag: %w", err)
	}

	// Commit the transaction.
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	// Enqueue reindexing jobs for affected images.
	for _, imageUUID := range affectedImageUUIDs {
		if err := r.container.Worker.EnqueueReindexImage(ctx, imageUUID); err != nil {
			log.Error().Err(err).Str("imageUUID", imageUUID).Msg("failed to enqueue image reindex")
		}
	}

	// Invalidate the hierarchy cache.
	if err := r.deleteHierarchyFromCache(ctx); err != nil {
		log.Error().Err(err).Msg("failed to invalidate hierarchy cache")
	}

	return nil
}

func (r *TagRepository) rebuildClosureForSubtreeTx(ctx context.Context, tx pgx.Tx, rootID int64) error {
	// Delete closure rows for all tags in the subtree.
	_, err := tx.Exec(ctx, `
        WITH RECURSIVE subtree AS (
            SELECT id FROM tags WHERE id = $1
            UNION ALL
            SELECT t.id FROM tags t
            JOIN subtree s ON t.parent_id = s.id
        )
        DELETE FROM tag_closure
        WHERE descendant IN (SELECT id FROM subtree)
    `, rootID)

	if err != nil {
		return fmt.Errorf("error deleting closure for subtree: %w", err)
	}

	// Rebuild the closure rows for the subtree.
	_, err = tx.Exec(ctx, `
        WITH RECURSIVE subtree AS (
            SELECT id FROM tags WHERE id = $1
            UNION ALL
            SELECT t.id FROM tags t
            JOIN subtree s ON t.parent_id = s.id
        ),
        closure AS (
            -- Base: each tag is its own ancestor at depth 0.
            SELECT id AS ancestor, id AS descendant, 0 AS depth FROM subtree
            UNION ALL
            -- Recursive: for each descendant, add its parent's ancestors.
            SELECT p.ancestor, t.id AS descendant, p.depth + 1 AS depth
            FROM closure p
            JOIN tags t ON t.parent_id = p.descendant
            WHERE t.id IN (SELECT id FROM subtree)
        )
        INSERT INTO tag_closure (ancestor, descendant, depth)
        SELECT ancestor, descendant, depth FROM closure
    `, rootID)

	if err != nil {
		return fmt.Errorf("error rebuilding closure for subtree: %w", err)
	}

	return nil
}
