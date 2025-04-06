package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type TagCache struct {
	container *container.Container
}

func NewTagCache(container *container.Container) *TagCache {
	return &TagCache{
		container: container,
	}
}

// Insert adds a tag to the Redis cache
func (c *TagCache) Insert(ctx context.Context, tag *models.Tag) error {
	// Store the tag hash
	hashKey := fmt.Sprintf("tag:%d", tag.ID)

	fields := tag.ToCacheFields()

	if err := c.container.Redis.Client.HSet(ctx, hashKey, fields).Err(); err != nil {
		return fmt.Errorf("failed to insert tag into redis: %w", err)
	}

	// Add tag to parent's sorted set
	var parentKey string
	if tag.ParentID != nil {
		parentKey = fmt.Sprintf("children:%d", *tag.ParentID)
	} else {
		parentKey = "children:root"
	}

	z := redis.Z{
		Score:  float64(tag.Position),
		Member: tag.ID,
	}

	if err := c.container.Redis.Client.ZAdd(ctx, parentKey, z).Err(); err != nil {
		return fmt.Errorf("failed to add tag to parent's sorted set in redis: %w", err)
	}

	return nil
}

// GetTag retrieves a single tag from the cache
func (c *TagCache) GetTag(ctx context.Context, id int64) (*models.Tag, error) {
	hashKey := fmt.Sprintf("tag:%d", id)
	fields, err := c.container.Redis.Client.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get tag from redis: %w", err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("tag with ID %d not found in cache", id)
	}

	return mapToTag(fields)
}

// GetChildren retrieves the direct children of a tag
func (c *TagCache) GetChildren(ctx context.Context, parentID *int64) ([]*models.Tag, error) {
	var parentKey string
	if parentID != nil {
		parentKey = fmt.Sprintf("children:%d", *parentID)
	} else {
		parentKey = "children:root"
	}

	// Get children IDs with scores (positions)
	childIDs, err := c.container.Redis.Client.ZRange(ctx, parentKey, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get child tag IDs from redis: %w", err)
	}

	if len(childIDs) == 0 {
		return []*models.Tag{}, nil
	}

	// Use pipelining to get all children in one round trip
	pipe := c.container.Redis.Client.Pipeline()
	cmds := make(map[string]*redis.MapStringStringCmd)

	for _, idStr := range childIDs {
		hashKey := fmt.Sprintf("tag:%s", idStr)
		cmds[idStr] = pipe.HGetAll(ctx, hashKey)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute pipeline for tag children: %w", err)
	}

	// Convert results to Tag models
	children := make([]*models.Tag, 0, len(childIDs))
	for id, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil {
			log.Error().Err(err).Str("id", id).Msg("Failed to get tag from pipeline")
			continue
		}

		if len(fields) == 0 {
			log.Warn().Str("id", id).Msg("Tag found in sorted set but hash not found")
			continue
		}

		tag, err := mapToTag(fields)
		if err != nil {
			log.Error().Err(err).Str("id", id).Msg("Failed to convert redis hash to tag")
			continue
		}

		children = append(children, tag)
	}

	return children, nil
}

// GetTagTree retrieves a complete tag tree from the specified parent ID down to a maximum depth
func (c *TagCache) GetTagTree(ctx context.Context, parentID *int64, maxDepth int) (map[int64][]*models.Tag, error) {
	if maxDepth < 0 {
		return nil, fmt.Errorf("maxDepth must be non-negative")
	}

	// Initialize the result map where key = parent ID, value = children tags
	result := make(map[int64][]*models.Tag)

	// Define a recursive function to fetch descendants
	var fetchDescendants func(parentID *int64, currentDepth int) error
	fetchDescendants = func(parentID *int64, currentDepth int) error {
		// If we've reached max depth, stop recursion
		if maxDepth > 0 && currentDepth >= maxDepth {
			return nil
		}

		// Get children of this parent
		children, err := c.GetChildren(ctx, parentID)
		if err != nil {
			return fmt.Errorf("failed to get children: %w", err)
		}

		if len(children) == 0 {
			return nil
		}

		// Add children to the result map
		var parentKey int64
		if parentID == nil {
			// Use 0 as special key for root tags
			parentKey = 0
		} else {
			parentKey = *parentID
		}

		result[parentKey] = children

		// Process each child recursively if we haven't reached max depth
		for _, child := range children {
			if err := fetchDescendants(&child.ID, currentDepth+1); err != nil {
				return err
			}
		}

		return nil
	}

	// Start the recursive process
	if err := fetchDescendants(parentID, 0); err != nil {
		return nil, err
	}

	return result, nil
}

// Update updates an existing tag in the cache
func (c *TagCache) Update(ctx context.Context, tag *models.Tag, oldParentID *int64) error {
	// Update the hash
	hashKey := fmt.Sprintf("tag:%d", tag.ID)
	fields := tag.ToCacheFields()

	if err := c.container.Redis.Client.HSet(ctx, hashKey, fields).Err(); err != nil {
		return fmt.Errorf("failed to update tag hash in redis: %w", err)
	}

	// If parent has changed, update the sorted sets
	if (oldParentID == nil && tag.ParentID != nil) ||
		(oldParentID != nil && tag.ParentID == nil) ||
		(oldParentID != nil && tag.ParentID != nil && *oldParentID != *tag.ParentID) {

		// Remove from old parent's sorted set
		var oldParentKey string
		if oldParentID != nil {
			oldParentKey = fmt.Sprintf("children:%d", *oldParentID)
		} else {
			oldParentKey = "children:root"
		}

		if err := c.container.Redis.Client.ZRem(ctx, oldParentKey, tag.ID).Err(); err != nil {
			return fmt.Errorf("failed to remove tag from old parent's sorted set: %w", err)
		}

		// Add to new parent's sorted set
		var newParentKey string
		if tag.ParentID != nil {
			newParentKey = fmt.Sprintf("children:%d", *tag.ParentID)
		} else {
			newParentKey = "children:root"
		}

		z := redis.Z{
			Score:  float64(tag.Position),
			Member: tag.ID,
		}

		if err := c.container.Redis.Client.ZAdd(ctx, newParentKey, z).Err(); err != nil {
			return fmt.Errorf("failed to add tag to new parent's sorted set: %w", err)
		}
	} else {
		// Just update the position if parent hasn't changed
		var parentKey string
		if tag.ParentID != nil {
			parentKey = fmt.Sprintf("children:%d", *tag.ParentID)
		} else {
			parentKey = "children:root"
		}

		z := redis.Z{
			Score:  float64(tag.Position),
			Member: tag.ID,
		}

		if err := c.container.Redis.Client.ZAdd(ctx, parentKey, z).Err(); err != nil {
			return fmt.Errorf("failed to update tag position in sorted set: %w", err)
		}
	}

	return nil
}

func (c *TagCache) Delete(ctx context.Context, tag *models.Tag, recursive bool) error {
	// If recursive, first get all children
	if recursive {
		children, err := c.GetChildren(ctx, &tag.ID)
		if err != nil {
			return fmt.Errorf("failed to get children for recursive delete: %w", err)
		}

		// Recursively delete all children
		for _, child := range children {
			if err := c.Delete(ctx, child, true); err != nil {
				log.Error().Err(err).Int64("id", child.ID).Msg("Error deleting child tag in recursive delete")
			}
		}
	}

	// Delete the tag hash
	hashKey := fmt.Sprintf("tag:%d", tag.ID)
	if err := c.container.Redis.Client.Del(ctx, hashKey).Err(); err != nil {
		return fmt.Errorf("failed to delete tag hash from redis: %w", err)
	}

	// Remove from parent's sorted set
	var parentKey string
	if tag.ParentID != nil {
		parentKey = fmt.Sprintf("children:%d", *tag.ParentID)
	} else {
		parentKey = "children:root"
	}

	if err := c.container.Redis.Client.ZRem(ctx, parentKey, tag.ID).Err(); err != nil {
		return fmt.Errorf("failed to remove tag from parent's sorted set: %w", err)
	}

	// Delete the children sorted set for this tag
	childrenKey := fmt.Sprintf("children:%d", tag.ID)
	if err := c.container.Redis.Client.Del(ctx, childrenKey).Err(); err != nil {
		return fmt.Errorf("failed to delete tag's children sorted set: %w", err)
	}

	log.Debug().Int64("id", tag.ID).Str("name", tag.Name).Msg("Tag deleted from cache")

	return nil
}

// Helper function to convert Redis hash map to Tag
func mapToTag(fields map[string]string) (*models.Tag, error) {
	tag := &models.Tag{}

	// Parse required fields
	idStr, ok := fields["id"]
	if !ok {
		return nil, fmt.Errorf("missing id field in redis hash")
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid id value in redis hash: %w", err)
	}
	tag.ID = id

	tag.UUID = fields["uuid"]
	tag.Name = fields["name"]

	// Parse position
	positionStr, ok := fields["position"]
	if ok {
		position, err := strconv.ParseInt(positionStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid position value in redis hash: %w", err)
		}
		tag.Position = int32(position)
	}

	// Parse optional fields
	if descStr, ok := fields["description"]; ok && descStr != "" {
		tag.Description = &descStr
	}

	// Parse parent_id
	if parentIDStr, ok := fields["parent_id"]; ok && parentIDStr != "" {
		parentID, err := strconv.ParseInt(parentIDStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid parent_id value in redis hash: %w", err)
		}
		tag.ParentID = &parentID
	}

	// Parse timestamps
	if createdAtStr, ok := fields["created_at"]; ok && createdAtStr != "" {
		createdAt, err := time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("invalid created_at value in redis hash: %w", err)
		}
		tag.CreatedAt = createdAt
	}

	if updatedAtStr, ok := fields["updated_at"]; ok && updatedAtStr != "" {
		updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
		if err != nil {
			return nil, fmt.Errorf("invalid updated_at value in redis hash: %w", err)
		}
		tag.UpdatedAt = updatedAt
	}

	return tag, nil
}
