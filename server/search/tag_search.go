package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	elastic_search "github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/rs/zerolog/log"
)

const TagIndex = "tags"

type TagSearch struct {
	container *container.Container
}

func NewTagSearch(container *container.Container) *TagSearch {
	return &TagSearch{
		container: container,
	}
}

func (s *TagSearch) Index(ctx context.Context, record *models.TagSearchRecord) error {
	// Marshal the document
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("error marshalling document: %w", err)
	}

	// Create index request
	req := esapi.IndexRequest{
		Index:      TagIndex,
		DocumentID: record.UUID,
		Body:       bytes.NewReader(payload),
		// Make the document immediately searchable
		Refresh: "true",
	}

	// Execute the request
	res, err := req.Do(ctx, s.container.Elastic.Client)
	if err != nil {
		return fmt.Errorf("error executing index request: %w", err)
	}

	// Handle potential close error
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close Elasticsearch response body")
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

func (s *TagSearch) Delete(ctx context.Context, uuid string) error {
	req := esapi.DeleteRequest{
		Index:      TagIndex,
		DocumentID: uuid,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, s.container.Elastic.Client)
	if err != nil {
		return fmt.Errorf("error executing delete request: %w", err)
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
				return fmt.Errorf("error parsing error response: %w", err)
			} else {
				return fmt.Errorf("error deleting document from index [status: %s]: %v", res.Status(), e)
			}
		}
	}

	return nil
}

type TagSortBy string

// Sort field constants for people
const (
	TagSortByRelevance TagSortBy = "_score"
	TagSortByCreatedAt TagSortBy = "created_at"
	TagSortByName      TagSortBy = "name.keyword"
)

type TagSearchOptions struct {
	// Search
	Name        string
	Description string

	// Filters
	SinceDate  *time.Time // Records created after this date
	BeforeDate *time.Time // Records created before this date
	ParentUUID *string    // Filter by parent UUID

	// Sorting
	SortBy        TagSortBy
	SortDirection utils.SortDirection

	// Pagination
	utils.PaginationOptions
}

type TagSearchResult struct {
	Results    []*models.TagSearchRecord
	HasMore    bool
	TotalCount int64
	NextCursor []types.FieldValue
}

func (s *TagSearch) Search(ctx context.Context, options *TagSearchOptions) (*TagSearchResult, error) {
	// Normalize the limit value
	limit := options.Limit
	if limit <= 0 {
		limit = 50 // default
	} else if limit > 100 {
		limit = 100 // max
	}

	// Build the Elasticsearch query
	query, err := s.prepareSearchQuery(options, limit)
	if err != nil {
		return nil, fmt.Errorf("error building search query: %w", err)
	}

	// Execute the search
	res, err := s.container.Elastic.Client.Search().Index(TagIndex).Request(query).TrackTotalHits(true).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("error executing search: %w", err)
	}

	// Extract total count
	totalHits := res.Hits.Total.Value
	hits := res.Hits.Hits

	// Determine if we have more results by checking if we have one extra hit
	hasMore := len(hits) > limit
	if hasMore {
		hits = hits[:limit] // Remove the extra hit from the data set
	}

	// Convert hits to models
	tags := make([]*models.TagSearchRecord, 0, len(hits))
	var nextCursor []types.FieldValue
	for i, hit := range hits {
		tag, err := s.hitToTag(hit)
		if err != nil {
			return nil, fmt.Errorf("error converting hit to tag: %w", err)
		}
		tags = append(tags, tag)

		// If this is the last hit and there are more results, use its "sort" field as the cursor.
		if i == len(hits)-1 && hasMore {
			nextCursor = append(nextCursor, hit.Sort...)
		}
	}

	return &TagSearchResult{
		Results:    tags,
		HasMore:    hasMore,
		TotalCount: totalHits,
		NextCursor: nextCursor,
	}, nil
}

func (s *TagSearch) prepareSearchQuery(options *TagSearchOptions, limit int) (*elastic_search.Request, error) {
	// Build query clause slices.
	var filters []types.Query
	var shoulds []types.Query

	// Apply name filter
	if options.Name != "" {
		shoulds = append(shoulds, types.Query{
			Match: map[string]types.MatchQuery{
				"name": {
					Query: options.Name,
					Boost: utils.NewPointer(float32(2.0)),
				},
			},
		})
	}

	// Apply description filter
	if options.Description != "" {
		shoulds = append(shoulds, types.Query{
			Match: map[string]types.MatchQuery{
				"description": {
					Query: options.Description,
				},
			},
		})
	}

	// Filter by parent UUID
	if options.ParentUUID != nil && *options.ParentUUID != "" {
		filters = append(filters, types.Query{
			Term: map[string]types.TermQuery{
				"parent_uuid": {Value: *options.ParentUUID},
			},
		})
	}

	// Apply date filters
	if options.SinceDate != nil || options.BeforeDate != nil {
		dateRange := types.DateRangeQuery{}

		if options.SinceDate != nil {
			dateRange.Gte = utils.NewPointer(options.SinceDate.Format(time.RFC3339))
		}
		if options.BeforeDate != nil {
			dateRange.Lte = utils.NewPointer(options.BeforeDate.Format(time.RFC3339))
		}

		filters = append(filters, types.Query{
			Range: map[string]types.RangeQuery{
				"created_at": dateRange,
			},
		})
	}

	// Determine sort direction
	var sortDirection sortorder.SortOrder
	switch options.SortDirection {
	case utils.SortDirectionAsc:
		sortDirection = sortorder.Asc
	default:
		sortDirection = sortorder.Desc
	}

	sortField := string(TagSortByCreatedAt)
	if options.SortBy != "" {
		sortField = string(options.SortBy)
	}

	// Build the search request based on the sort field
	searchRequest := &elastic_search.Request{
		Size: utils.NewPointer(limit + 1),
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must:   filters,
				Should: shoulds,
			},
		},
		Sort: []types.SortCombinations{
			types.SortOptions{
				SortOptions: map[string]types.FieldSort{
					sortField: {
						Order: &sortDirection,
					},
					"id": {
						Order: &sortorder.Asc,
					},
				},
			},
		},
	}

	// If a StartingAfter cursor is provided, attach it
	if options.StartingAfter != nil {
		searchRequest.SearchAfter = options.StartingAfter
	}

	return searchRequest, nil
}

// rawTagSearchRecord is a helper type for unmarshalling the Elasticsearch hit source.
type rawTagSearchRecord struct {
	ID          float64 `json:"id"`
	UUID        string  `json:"uuid"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
	ParentID    *int64  `json:"parent_id"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

func (s *TagSearch) hitToTag(hit types.Hit) (*models.TagSearchRecord, error) {
	var raw rawTagSearchRecord
	if err := json.Unmarshal(hit.Source_, &raw); err != nil {
		return nil, fmt.Errorf("error unmarshalling hit source: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339, raw.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("error parsing created_at: %w", err)
	}

	updatedAt, err := time.Parse(time.RFC3339, raw.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("error parsing updated_at: %w", err)
	}

	return &models.TagSearchRecord{
		ID:          int64(raw.ID),
		UUID:        raw.UUID,
		Name:        raw.Name,
		Description: raw.Description,
		ParentID:    raw.ParentID,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}
