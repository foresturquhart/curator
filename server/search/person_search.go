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
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/functionboostmode"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/rs/zerolog/log"
)

type PersonSearch struct {
	container *container.Container
}

func NewPersonSearch(container *container.Container) *PersonSearch {
	return &PersonSearch{
		container: container,
	}
}

func (s *PersonSearch) Delete(ctx context.Context, uuid string) error {
	req := esapi.DeleteRequest{
		Index:      "people",
		DocumentID: uuid,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, s.container.Elastic.Client)
	if err != nil {
		return fmt.Errorf("error executing delete request: %w", err)
	}

	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			log.Error().Err(err).Msg("Failed to close Elasticsearch response body")
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

func (s *PersonSearch) Index(ctx context.Context, person *models.Person) error {
	// Construct the document to index
	document := map[string]any{
		"id":         person.ID,
		"uuid":       person.UUID,
		"name":       person.Name,
		"created_at": person.CreatedAt,
		"updated_at": person.UpdatedAt,
	}

	// Handle nullable fields
	if person.Description != nil {
		document["description"] = *person.Description
	}

	// Add sources
	if len(person.Sources) > 0 {
		sources := make([]map[string]any, len(person.Sources))
		for i, source := range person.Sources {
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
		Index:      "people",
		DocumentID: person.UUID,
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

func (s *PersonSearch) Search(ctx context.Context, filter *models.PersonFilter) (*models.PaginatedPersonResult, error) {
	// Normalize the limit value
	limit := filter.Limit
	if limit <= 0 {
		limit = 50 // default
	} else if limit > 100 {
		limit = 100 // max
	}

	// Build the Elasticsearch query
	query, err := s.prepareSearchQuery(filter, limit)
	if err != nil {
		return nil, fmt.Errorf("error building search query: %w", err)
	}

	// Execute the search
	res, err := s.container.Elastic.Client.Search().Index("people").Request(query).TrackTotalHits(true).Do(ctx)
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
	people := make([]*models.Person, 0, len(hits))
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

	// Use the pagination helper to format the response
	return &models.PaginatedPersonResult{
		Data:       people,
		HasMore:    hasMore,
		TotalCount: totalHits,
		NextCursor: nextCursor,
	}, nil
}

func (s *PersonSearch) prepareSearchQuery(filter *models.PersonFilter, limit int) (*elastic_search.Request, error) {
	// Build query clause slices.
	var filters []types.Query
	var shoulds []types.Query

	// Apply name filter
	if filter.Name != "" {
		shoulds = append(shoulds, types.Query{
			Match: map[string]types.MatchQuery{
				"name": {
					Query: filter.Name,
					Boost: utils.NewPointer(float32(2.0)),
				},
			},
		})
	}

	// Apply description filter
	if filter.Description != "" {
		shoulds = append(shoulds, types.Query{
			Match: map[string]types.MatchQuery{
				"description": {
					Query: filter.Description,
				},
			},
		})
	}

	// Apply source filter
	if filter.Source != "" {
		shoulds = append(shoulds, types.Query{
			Nested: &types.NestedQuery{
				Path: "sources",
				Query: &types.Query{
					Bool: &types.BoolQuery{
						Should: []types.Query{
							{
								Term: map[string]types.TermQuery{
									"sources.url.keyword": {
										Value: filter.Source,
										Boost: utils.NewPointer(float32(2.0)), // Higher boost for exact matches
									},
								},
							},
							{
								Match: map[string]types.MatchQuery{
									"sources.url": {
										Query: filter.Source,
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
	if filter.SinceDate != nil || filter.BeforeDate != nil {
		dateRange := types.DateRangeQuery{}

		if filter.SinceDate != nil {
			dateRange.Gte = utils.NewPointer(filter.SinceDate.Format(time.RFC3339))
		}
		if filter.BeforeDate != nil {
			dateRange.Lte = utils.NewPointer(filter.BeforeDate.Format(time.RFC3339))
		}

		filters = append(filters, types.Query{
			Range: map[string]types.RangeQuery{
				"created_at": dateRange,
			},
		})
	}

	finalBoolQuery := &types.BoolQuery{
		Must:   filters,
		Should: shoulds,
	}

	// Determine sort direction
	var sortDirection sortorder.SortOrder
	switch filter.SortDirection {
	case models.SortDirectionAsc:
		sortDirection = sortorder.Asc
	default:
		sortDirection = sortorder.Desc
	}

	// Build the search request based on the sort field
	var searchRequest *elastic_search.Request

	switch filter.SortBy {
	case models.PersonSortByCreatorCount:
		// When sorting by creator count, use a function_score with a script
		searchRequest = &elastic_search.Request{
			Size: utils.NewPointer(limit + 1),
			Query: &types.Query{
				FunctionScore: &types.FunctionScoreQuery{
					Query: &types.Query{Bool: finalBoolQuery},
					Functions: []types.FunctionScore{
						{
							ScriptScore: &types.ScriptScoreFunction{
								Script: types.Script{
									Source: utils.NewPointer(`
										int count = 0;
										if (doc.containsKey('uuid')) {
											def uuid = doc['uuid'].value;
											count = searchIndex('images', 'people.uuid:' + uuid + ' AND people.role:creator').count();
										}
										return count;
									`),
								},
							},
						},
					},
					BoostMode: &functionboostmode.Replace,
				},
			},
			Sort: []types.SortCombinations{
				types.SortOptions{
					SortOptions: map[string]types.FieldSort{
						"_score": {
							Order: &sortDirection,
						},
					},
				},
			},
		}

	case models.PersonSortBySubjectCount:
		// Similar approach for subject count
		searchRequest = &elastic_search.Request{
			Size: utils.NewPointer(limit + 1),
			Query: &types.Query{
				FunctionScore: &types.FunctionScoreQuery{
					Query: &types.Query{Bool: finalBoolQuery},
					Functions: []types.FunctionScore{
						{
							ScriptScore: &types.ScriptScoreFunction{
								Script: types.Script{
									Source: utils.NewPointer(`
										int count = 0;
										if (doc.containsKey('uuid')) {
											def uuid = doc['uuid'].value;
											count = searchIndex('images', 'people.uuid:' + uuid + ' AND people.role:subject').count();
										}
										return count;
									`),
								},
							},
						},
					},
					BoostMode: &functionboostmode.Replace,
				},
			},
			Sort: []types.SortCombinations{
				types.SortOptions{
					SortOptions: map[string]types.FieldSort{
						"_score": {
							Order: &sortDirection,
						},
					},
				},
			},
		}

	default:
		// Standard sorting for other fields
		sortField := string(models.PersonSortByCreatedAt)
		if filter.SortBy != "" {
			sortField = string(filter.SortBy)
		} else if filter.Name != "" || filter.Description != "" || filter.Source != "" {
			sortField = string(models.PersonSortByRelevance)
		}

		searchRequest = &elastic_search.Request{
			Size:  utils.NewPointer(limit + 1), // Extra document to detect more pages
			Query: &types.Query{Bool: finalBoolQuery},
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
	}

	// If a StartingAfter cursor is provided, attach it
	if filter.StartingAfter != nil {
		searchRequest.SearchAfter = filter.StartingAfter
	}

	return searchRequest, nil
}

// hitToPerson converts an Elasticsearch hit to a Person model
func (s *PersonSearch) hitToPerson(hit types.Hit) (*models.Person, error) {
	log.Debug().Interface("score", hit.Score_).Interface("uuid", hit.Id_).Msg("Parsing Elasticsearch hit")

	// Parse the source
	var source map[string]any
	err := json.Unmarshal(hit.Source_, &source)
	if err != nil {
		return nil, fmt.Errorf("error parsing source: %w", err)
	}

	// Inline helper functions for extracting fields.
	getString := func(key string) (string, error) {
		val, exists := source[key]
		if !exists || val == nil {
			return "", fmt.Errorf("missing %s", key)
		}
		s, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("field %s is not a string", key)
		}
		return s, nil
	}

	getFloat64 := func(key string) (float64, error) {
		val, exists := source[key]
		if !exists || val == nil {
			return 0, fmt.Errorf("missing %s", key)
		}
		f, ok := val.(float64)
		if !ok {
			return 0, fmt.Errorf("field %s is not a float64", key)
		}
		return f, nil
	}

	parseTimeField := func(key string) (time.Time, error) {
		str, err := getString(key)
		if err != nil {
			return time.Time{}, err
		}
		return time.Parse(time.RFC3339, str)
	}

	// Parse required fields.
	idFloat, err := getFloat64("id")
	if err != nil {
		return nil, err
	}

	uuid, err := getString("uuid")
	if err != nil {
		return nil, err
	}

	name, err := getString("name")
	if err != nil {
		return nil, err
	}

	createdAt, err := parseTimeField("created_at")
	if err != nil {
		return nil, fmt.Errorf("error parsing created_at: %w", err)
	}

	updatedAt, err := parseTimeField("updated_at")
	if err != nil {
		return nil, fmt.Errorf("error parsing updated_at: %w", err)
	}

	// Build the base person.
	person := &models.Person{
		ID:        int64(idFloat),
		UUID:      uuid,
		Name:      name,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	// Set description if present
	if desc, exists := source["description"]; exists && desc != nil {
		if descStr, ok := desc.(string); ok {
			person.Description = &descStr
		}
	}

	// Process sources if available.
	if sourcesRaw, exists := source["sources"]; exists && sourcesRaw != nil {
		sourcesArr, ok := sourcesRaw.([]interface{})
		if ok {
			sources := make([]*models.PersonSource, 0, len(sourcesArr))
			for _, rawSource := range sourcesArr {
				srcMap, ok := rawSource.(map[string]interface{})
				if !ok {
					continue
				}
				url, ok := srcMap["url"].(string)
				if !ok {
					return nil, fmt.Errorf("source url is not a string")
				}
				personSource := &models.PersonSource{URL: url}
				if t, ok := srcMap["title"].(string); ok {
					personSource.Title = &t
				}
				if d, ok := srcMap["description"].(string); ok {
					personSource.Description = &d
				}
				sources = append(sources, personSource)
			}
			person.Sources = sources
		}
	}

	return person, nil
}
