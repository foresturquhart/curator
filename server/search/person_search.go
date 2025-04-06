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

const PeopleIndex = "people"

type PersonSearch struct {
	container *container.Container
}

func NewPersonSearch(container *container.Container) *PersonSearch {
	return &PersonSearch{
		container: container,
	}
}

// Delete removes a document from the Elasticsearch index based on the provided UUID.
func (s *PersonSearch) Delete(ctx context.Context, uuid string) error {
	req := esapi.DeleteRequest{
		Index:      PeopleIndex,
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

// Index adds or updates a PersonSearchRecord in the Elasticsearch index.
func (s *PersonSearch) Index(ctx context.Context, record *models.PersonSearchRecord) error {
	// Construct the document to index
	document := map[string]any{
		"id":         record.ID,
		"uuid":       record.UUID,
		"name":       record.Name,
		"created_at": record.CreatedAt,
		"updated_at": record.UpdatedAt,
	}

	// Handle nullable fields
	if record.Description != nil {
		document["description"] = *record.Description
	}

	// Add sources
	if len(record.Sources) > 0 {
		sources := make([]map[string]any, len(record.Sources))
		for i, source := range record.Sources {
			sourceDoc := map[string]any{
				"url": source.URL,
			}

			// Handle nullable fields
			if source.Title != nil {
				sourceDoc["title"] = *source.Title
			}

			if source.Description != nil {
				sourceDoc["description"] = *source.Description
			}

			sources[i] = sourceDoc
		}
		document["sources"] = sources
	}

	// Encode the document
	payload, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("error encoding document: %w", err)
	}

	// Create index request
	req := esapi.IndexRequest{
		Index:      PeopleIndex,
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

// SortBy specifies the field to sort by
type PersonSortBy string

// Sort field constants for people
const (
	PersonSortByRelevance PersonSortBy = "_score"
	PersonSortByCreatedAt PersonSortBy = "created_at"
	PersonSortByName      PersonSortBy = "name.keyword"
)

type PersonSearchOptions struct {
	// Search
	Name        string
	Description string

	// Filters
	Source     string     // Filter by source URL
	SinceDate  *time.Time // Records created after this date
	BeforeDate *time.Time // Records created before this date

	// Sorting
	SortBy        PersonSortBy
	SortDirection utils.SortDirection

	// Pagination
	utils.PaginationOptions
}

type PersonSearchResult struct {
	Results    []*models.PersonSearchRecord
	HasMore    bool
	TotalCount int64
	NextCursor []types.FieldValue
}

// Search executes an Elasticsearch query based on the provided filter, sort, and pagination options.
func (s *PersonSearch) Search(ctx context.Context, options *PersonSearchOptions) (*PersonSearchResult, error) {
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
	res, err := s.container.Elastic.Client.Search().Index(PeopleIndex).Request(query).TrackTotalHits(true).Do(ctx)
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
	people := make([]*models.PersonSearchRecord, 0, len(hits))
	var nextCursor []types.FieldValue
	for i, hit := range hits {
		person, err := s.hitToPerson(hit)
		if err != nil {
			return nil, fmt.Errorf("error converting hit to person: %w", err)
		}
		people = append(people, person)

		// If this is the last hit and there are more results, use its "sort" field as the cursor.
		if i == len(hits)-1 && hasMore {
			nextCursor = append(nextCursor, hit.Sort...)
		}
	}

	return &PersonSearchResult{
		Results:    people,
		HasMore:    hasMore,
		TotalCount: totalHits,
		NextCursor: nextCursor,
	}, nil
}

func (s *PersonSearch) prepareSearchQuery(options *PersonSearchOptions, limit int) (*elastic_search.Request, error) {
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

	// Apply source filter
	if options.Source != "" {
		shoulds = append(shoulds, types.Query{
			Nested: &types.NestedQuery{
				Path: "sources",
				Query: &types.Query{
					Bool: &types.BoolQuery{
						Should: []types.Query{
							{
								Term: map[string]types.TermQuery{
									"sources.url.keyword": {
										Value: options.Source,
										Boost: utils.NewPointer(float32(2.0)), // Higher boost for exact matches
									},
								},
							},
							{
								Match: map[string]types.MatchQuery{
									"sources.url": {
										Query: options.Source,
										Boost: utils.NewPointer(float32(1.5)), // Lower boost for partial matches
									},
								},
							},
						},
					},
				},
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

	sortField := string(PersonSortByCreatedAt)
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

// rawPersonSearchRecord is a helper type for unmarshalling the Elasticsearch hit source.
type rawPersonSearchRecord struct {
	ID          float64                       `json:"id"`
	UUID        string                        `json:"uuid"`
	Name        string                        `json:"name"`
	Description *string                       `json:"description"`
	CreatedAt   string                        `json:"created_at"`
	UpdatedAt   string                        `json:"updated_at"`
	Sources     []rawPersonSearchRecordSource `json:"sources"`
}

// rawPersonSearchRecordSource mirrors PersonSearchRecordSource for unmarshalling.
type rawPersonSearchRecordSource struct {
	URL         string  `json:"url"`
	Title       *string `json:"title"`
	Description *string `json:"description"`
}

func (s *PersonSearch) hitToPerson(hit types.Hit) (*models.PersonSearchRecord, error) {
	var raw rawPersonSearchRecord
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

	// Convert raw sources to domain sources.
	var sources []*models.PersonSearchRecordSource
	for _, src := range raw.Sources {
		sources = append(sources, &models.PersonSearchRecordSource{
			URL:         src.URL,
			Title:       src.Title,
			Description: src.Description,
		})
	}

	return &models.PersonSearchRecord{
		ID:          int64(raw.ID),
		UUID:        raw.UUID,
		Name:        raw.Name,
		Description: raw.Description,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		Sources:     sources,
	}, nil
}
