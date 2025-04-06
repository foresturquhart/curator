package services

import (
	"context"
	"fmt"

	"github.com/foresturquhart/curator/server/cache"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/search"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/rs/zerolog/log"
)

type TagService struct {
	container *container.Container
	repo      *repositories.TagRepository
	search    *search.TagSearch
	cache     *cache.TagCache
}

func NewTagService(container *container.Container) *TagService {
	return &TagService{
		container: container,
		repo:      repositories.NewTagRepository(container),
		search:    search.NewTagSearch(container),
		cache:     cache.NewTagCache(container),
	}
}

func (s *TagService) Get(ctx context.Context, uuid string) (*models.Tag, error) {
	return s.repo.GetByUUID(ctx, uuid)
}

func (s *TagService) GetByInternalID(ctx context.Context, id int64) (*models.Tag, error) {
	return s.repo.GetByInternalID(ctx, id)
}

func (s *TagService) Create(ctx context.Context, tag *models.Tag, opts repositories.TagCreateOptions) error {
	if err := s.repo.Create(ctx, tag, opts); err != nil {
		return fmt.Errorf("failed to create tag: %w", err)
	}

	if err := s.cache.Insert(ctx, tag); err != nil {
		log.Error().Err(err).Msgf("Failed to cache tag %s", tag.UUID)
	}

	if err := s.search.Index(ctx, tag.ToSearchRecord()); err != nil {
		log.Error().Err(err).Msgf("Failed to index tag %s", tag.UUID)
	}

	return nil
}

func (s *TagService) Update(ctx context.Context, tag *models.Tag, opts *repositories.TagUpdateOptions) error {
	var oldTag *models.Tag
	var err error

	if tag.ID > 0 {
		oldTag, err = s.repo.GetByInternalID(ctx, tag.ID)
	} else {
		oldTag, err = s.repo.GetByUUID(ctx, tag.UUID)
	}

	if err != nil {
		return fmt.Errorf("failed to retrieve existing tag: %w", err)
	}

	affectedImages, err := s.repo.Update(ctx, tag, opts)
	if err != nil {
		return fmt.Errorf("failed to update tag: %w", err)
	}

	// Update in cache
	if err := s.cache.Update(ctx, tag, oldTag.ParentID); err != nil {
		log.Error().Err(err).Msgf("Failed to update tag %s in cache", tag.UUID)
	}

	if err := s.search.Index(ctx, tag.ToSearchRecord()); err != nil {
		log.Error().Err(err).Msgf("Failed to index tag %s", tag.UUID)
	}

	for _, affectedImage := range affectedImages {
		if err := s.container.Worker.EnqueueReindexImage(ctx, affectedImage); err != nil {
			log.Error().Err(err).Int64("id", affectedImage).Msg("Error reindexing image after tag update")
		}
	}

	return nil
}

func (s *TagService) Merge(ctx context.Context, source *models.Tag, destination *models.Tag) error {
	affectedImages, err := s.repo.Merge(ctx, source, destination)
	if err != nil {
		return fmt.Errorf("failed to merge tags: %w", err)
	}

	if err := s.search.Delete(ctx, source.UUID); err != nil {
		log.Error().Err(err).Msgf("Failed to delete tag %s from index", source.UUID)
	}

	// Remove source tag from cache
	if err := s.cache.Delete(ctx, source, false); err != nil {
		log.Error().Err(err).Msgf("Failed to delete tag %s from cache", source.UUID)
	}

	if err := s.search.Index(ctx, destination.ToSearchRecord()); err != nil {
		log.Error().Err(err).Msgf("Failed to index tag %s", destination.UUID)
	}

	// Update destination tag in cache
	if err := s.cache.Update(ctx, destination, destination.ParentID); err != nil {
		log.Error().Err(err).Msgf("Failed to update tag %s in cache", destination.UUID)
	}

	for _, affectedImage := range affectedImages {
		if err := s.container.Worker.EnqueueReindexImage(ctx, affectedImage); err != nil {
			log.Error().Err(err).Int64("id", affectedImage).Msg("Error reindexing image after tag merge")
		}
	}

	return nil
}

func (s *TagService) Delete(ctx context.Context, tag *models.Tag) error {
	affectedImages, err := s.repo.Delete(ctx, tag)
	if err != nil {
		return fmt.Errorf("failed to delet tag: %w", err)
	}

	// Delete from cache
	if err := s.cache.Delete(ctx, tag, true); err != nil {
		log.Error().Err(err).Msgf("Failed to delete tag %s from cache", tag.UUID)
	}

	if err := s.search.Delete(ctx, tag.UUID); err != nil {
		log.Error().Err(err).Msgf("Failed to delete tag %s from index", tag.UUID)
	}

	for _, affectedImage := range affectedImages {
		if err := s.container.Worker.EnqueueReindexImage(ctx, affectedImage); err != nil {
			log.Error().Err(err).Int64("id", affectedImage).Msg("Error reindexing image after tag deletion")
		}
	}

	return nil
}

func (s *TagService) Tree(ctx context.Context, start *models.Tag, depth *int) ([]*models.TagTreeNode, error) {
	// Determine the starting parent ID
	var parentID *int64
	if start != nil {
		parentID = &start.ID
	}

	// Determine the maximum depth to traverse
	maxDepth := -1 // Default to unlimited depth
	if depth != nil {
		maxDepth = *depth
	}

	// Try to get the tree from cache first
	tagTreeMap, err := s.cache.GetTagTree(ctx, parentID, maxDepth)
	if err != nil {
		log.Warn().Err(err).
			Str("start_uuid", utils.ValueOrEmpty(start, func(t *models.Tag) string { return t.UUID })).
			Int("max_depth", maxDepth).
			Msg("Failed to get tag tree from cache, falling back to database")

		// Fall back to database queries for the tree
		return s.getTreeFromDatabase(ctx, parentID, maxDepth)
	}

	return s.buildTreeFromMap(parentID, tagTreeMap), nil
}

// buildTreeFromMap converts a map of parent IDs to children lists into a hierarchical tree
func (s *TagService) buildTreeFromMap(parentID *int64, tagTreeMap map[int64][]*models.Tag) []*models.TagTreeNode {
	// Determine the key to use for looking up children
	var key int64
	if parentID == nil {
		key = 0 // Special key for root tags
	} else {
		key = *parentID
	}

	// Get the children for this parent
	children, ok := tagTreeMap[key]
	if !ok || len(children) == 0 {
		return []*models.TagTreeNode{}
	}

	// Build the tree nodes for these children
	result := make([]*models.TagTreeNode, 0, len(children))
	for _, child := range children {
		node := &models.TagTreeNode{
			Tag: child,
		}

		// Recursively build children for this node
		node.Children = s.buildTreeFromMap(&child.ID, tagTreeMap)

		result = append(result, node)
	}

	return result
}

// getTreeFromDatabase builds the tree by making database queries
// This is a fallback method when the cache is not available
func (s *TagService) getTreeFromDatabase(ctx context.Context, parentID *int64, maxDepth int) ([]*models.TagTreeNode, error) {
	// Get children from the repository
	children, err := s.repo.GetChildren(ctx, parentID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tag children from database: %w", err)
	}

	// If we're at max depth or there are no children, return
	if maxDepth == 0 || len(children) == 0 {
		nodes := make([]*models.TagTreeNode, 0, len(children))
		for _, child := range children {
			nodes = append(nodes, &models.TagTreeNode{Tag: child})
		}
		return nodes, nil
	}

	// Build the tree recursively
	result := make([]*models.TagTreeNode, 0, len(children))
	for _, child := range children {
		node := &models.TagTreeNode{
			Tag: child,
		}

		// Recursively get children with decremented depth
		nextDepth := maxDepth
		if nextDepth > 0 {
			nextDepth--
		}

		childNodes, err := s.getTreeFromDatabase(ctx, &child.ID, nextDepth)
		if err != nil {
			log.Error().Err(err).Int64("id", child.ID).Msg("Error getting children for tag")
			continue
		}

		node.Children = childNodes
		result = append(result, node)
	}

	return result, nil
}

func (s *TagService) Search(ctx context.Context, options *search.TagSearchOptions) (*utils.PaginatedResult[*models.Tag], error) {
	result, err := s.search.Search(ctx, options)

	if err != nil {
		return nil, fmt.Errorf("failed to search for tags: %w", err)
	}

	var data []*models.Tag
	for _, result := range result.Results {
		data = append(data, result.ToModel())
	}

	return &utils.PaginatedResult[*models.Tag]{
		Data:       data,
		HasMore:    result.HasMore,
		TotalCount: result.TotalCount,
		NextCursor: result.NextCursor,
	}, nil
}

func (s *TagService) Index(ctx context.Context, tag *models.Tag) error {
	if err := s.search.Index(ctx, tag.ToSearchRecord()); err != nil {
		return fmt.Errorf("failed to index tag: %w", err)
	}

	return nil
}

func (s *TagService) IndexAll(ctx context.Context) error {
	tagIDs, err := s.repo.GetAllIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get person IDs: %w", err)
	}

	for _, id := range tagIDs {
		tag, err := s.repo.GetByInternalID(ctx, id)
		if err != nil {
			log.Error().Err(err).Msgf("Error retrieving tag for id %d", id)
			continue
		}

		if err := s.search.Index(ctx, tag.ToSearchRecord()); err != nil {
			log.Error().Err(err).Msgf("Error reindexing tag %s", tag.UUID)
			continue
		}

		log.Info().Msgf("Reindexed tag %s", tag.UUID)
	}

	return nil
}
