package repositories

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/functionboostmode"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
)

type PersonRepository struct {
	container *container.Container
}

func NewPersonRepository(container *container.Container) *PersonRepository {
	return &PersonRepository{
		container: container,
	}
}

func (r *PersonRepository) reindexElastic(ctx context.Context, person *models.Person) error {
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

func (r *PersonRepository) Reindex(ctx context.Context, person *models.Person) error {
	if err := r.reindexElastic(ctx, person); err != nil {
		return fmt.Errorf("error indexing person in Elastic: %w", err)
	}

	return nil
}

func (r *PersonRepository) ReindexAll(ctx context.Context) error {
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	// Get all person IDs
	rows, err := tx.Query(ctx, "SELECT id FROM people ORDER BY id")
	if err != nil {
		return fmt.Errorf("error querying person IDs: %w", err)
	}
	defer rows.Close()

	var personIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("error scanning person ID: %w", err)
		}
		personIDs = append(personIDs, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating person IDs: %w", err)
	}

	// Commit the transaction to release the connection
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction to get IDs: %w", err)
	}

	// Iterate through IDs and reindex each person
	for _, id := range personIDs {
		// Get the person by ID
		person, err := r.GetByID(ctx, id)
		if err != nil {
			// Log the error and continue to the next person
			log.Error().Err(err).Msgf("Error retrieving person for id %d", id)
			continue
		}

		// Reindex in a new transaction
		if err := r.Reindex(ctx, person); err != nil {
			log.Error().Err(err).Msgf("Error reindexing person %s", person.UUID)
			continue
		}

		log.Info().Msgf("Reindexed person %s", person.UUID)
	}

	return nil
}

func (r *PersonRepository) getByIDTx(ctx context.Context, tx pgx.Tx, id int64) (*models.Person, error) {
	query := `
		SELECT id, uuid, name, description, created_at, updated_at
		FROM people
		WHERE id = $1
	`

	var person models.Person
	var descriptionPtr *string

	err := tx.QueryRow(ctx, query, id).Scan(
		&person.ID, &person.UUID, &person.Name, &descriptionPtr, &person.CreatedAt, &person.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrPersonNotFound
		}
		return nil, fmt.Errorf("error fetching person: %w", err)
	}

	person.Description = descriptionPtr

	// Fetch all associations
	err = r.fetchPersonSources(ctx, tx, &person)
	if err != nil {
		return nil, err
	}

	return &person, nil
}

func (r *PersonRepository) GetByID(ctx context.Context, id int64) (*models.Person, error) {
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

	person, err := r.getByIDTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return person, nil
}

func (r *PersonRepository) getByUUIDTx(ctx context.Context, tx pgx.Tx, uuid string) (*models.Person, error) {
	query := `
		SELECT id, uuid, name, description, created_at, updated_at
		FROM people
		WHERE uuid = $1
	`

	var person models.Person
	var descriptionPtr *string

	err := tx.QueryRow(ctx, query, uuid).Scan(
		&person.ID, &person.UUID, &person.Name, &descriptionPtr, &person.CreatedAt, &person.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrPersonNotFound
		}
		return nil, fmt.Errorf("error fetching person: %w", err)
	}

	person.Description = descriptionPtr

	// Fetch all associations
	err = r.fetchPersonSources(ctx, tx, &person)
	if err != nil {
		return nil, err
	}

	return &person, nil
}

func (r *PersonRepository) GetByUUID(ctx context.Context, uuid string) (*models.Person, error) {
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

	person, err := r.getByUUIDTx(ctx, tx, uuid)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return person, nil
}

// GetByName finds a person by their exact name
func (r *PersonRepository) GetByName(ctx context.Context, name string) (*models.Person, error) {
	query := `
        SELECT id, uuid, name, description, created_at, updated_at
        FROM people
        WHERE name = $1
    `

	var person models.Person
	var descriptionPtr *string

	err := r.container.Database.Pool.QueryRow(ctx, query, name).Scan(
		&person.ID, &person.UUID, &person.Name, &descriptionPtr, &person.CreatedAt, &person.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Return nil, nil when no matching person is found
		}
		return nil, fmt.Errorf("error fetching person by name: %w", err)
	}

	person.Description = descriptionPtr

	return &person, nil
}

func (r *PersonRepository) Upsert(ctx context.Context, person *models.Person) error {
	// Check for duplicates by name (only for new records)
	if person.ID == 0 && person.UUID == "" {
		existingPerson, err := r.GetByName(ctx, person.Name)
		if err != nil {
			return fmt.Errorf("error checking for duplicate names: %w", err)
		}

		if existingPerson != nil {
			// Return a special error type that includes the conflicting person's UUID
			return &utils.ConflictError{
				Message:    "A person with this name already exists",
				ConflictID: existingPerson.UUID,
			}
		}
	}

	// Start a transaction
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
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

	// Get the existing person to compare associations
	var existingPerson *models.Person

	// Determine if this is an insert or update
	isUpdate := person.ID > 0 || person.UUID != ""

	if isUpdate {
		if person.ID > 0 {
			existingPerson, err = r.getByIDTx(ctx, tx, person.ID)
		} else {
			existingPerson, err = r.getByUUIDTx(ctx, tx, person.UUID)
		}

		if err != nil {
			return fmt.Errorf("error retrieving existing person: %w", err)
		}

		// Perform the update
		query := `
			UPDATE people SET
				name = $1,
				description = $2
			WHERE id = $3
			RETURNING id, uuid, created_at, updated_at
		`

		err = tx.QueryRow(
			ctx, query, person.Name, person.Description, existingPerson.ID,
		).Scan(&person.ID, &person.UUID, &person.CreatedAt, &person.UpdatedAt)

		if err != nil {
			return fmt.Errorf("error updating person: %w", err)
		}
	} else {
		// Create new person
		query := `
			INSERT INTO people (
				name, description
			) VALUES (
				$1, $2
			) RETURNING id, uuid, created_at, updated_at
		`

		err = tx.QueryRow(ctx, query,
			person.Name, person.Description,
		).Scan(&person.ID, &person.UUID, &person.CreatedAt, &person.UpdatedAt)

		if err != nil {
			return fmt.Errorf("error inserting person: %w", err)
		}
	}

	// Synchronize source associations
	if err := r.syncSourceAssociations(ctx, tx, person, existingPerson); err != nil {
		return fmt.Errorf("error handling source associations: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	// Enqueue reindex after successful storage commit
	if err := r.container.Worker.EnqueueReindexPerson(ctx, person.UUID); err != nil {
		log.Error().Err(err).Msgf("Failed to queue reindexing person %s", person.UUID)
	}

	return nil
}

// fetchPersonSources retrieves all sources associated with a person
func (r *PersonRepository) fetchPersonSources(ctx context.Context, tx pgx.Tx, person *models.Person) error {
	query := `
		SELECT
			s.url,
			s.title,
			s.description
		FROM person_sources s
		WHERE s.person_id = $1
		ORDER BY s.title, s.url;
	`

	rows, err := tx.Query(ctx, query, person.ID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var sources []*models.PersonSource
	for rows.Next() {
		var source models.PersonSource
		err := rows.Scan(&source.URL, &source.Title, &source.Description)
		if err != nil {
			return err
		}

		sources = append(sources, &source)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	person.Sources = sources
	return nil
}

// syncSourceAssociations synchronizes source associations for a person
func (r *PersonRepository) syncSourceAssociations(ctx context.Context, tx pgx.Tx, person *models.Person, existingPerson *models.Person) error {
	// Create map to track existing sources
	existingSourcesByURL := make(map[string]*models.PersonSource)

	if existingPerson != nil && existingPerson.Sources != nil {
		for _, source := range existingPerson.Sources {
			existingSourcesByURL[source.URL] = source
		}
	}

	// Map to track sources we need to retain
	sourcesToKeep := make(map[string]bool)

	// Process each source in the input model
	updatedSources := make([]*models.PersonSource, 0, len(person.Sources))

	for _, source := range person.Sources {
		if source == nil || source.URL == "" {
			continue
		}

		// Mark this source URL as one to keep
		sourcesToKeep[source.URL] = true

		// Check if this source URL is already associated with this person
		if _, exists := existingSourcesByURL[source.URL]; exists {
			// Source already exists - update its information
			query := `
                UPDATE person_sources
                SET title = $1, description = $2
                WHERE person_id = $3 AND url = $4
			`

			_, err := tx.Exec(ctx, query, source.Title, source.Description, person.ID, source.URL)
			if err != nil {
				return fmt.Errorf("error updating source: %w", err)
			}

			// Create updated source with the original timestamp
			updatedSource := &models.PersonSource{
				URL:         source.URL,
				Title:       source.Title,
				Description: source.Description,
			}

			updatedSources = append(updatedSources, updatedSource)
		} else {
			// New source - insert it
			query := `
                INSERT INTO person_sources (person_id, url, title, description)
                VALUES ($1, $2, $3, $4)
            `

			_, err := tx.Exec(ctx, query, person.ID, source.URL, source.Title, source.Description)
			if err != nil {
				return fmt.Errorf("error creating source: %w", err)
			}

			// Create updated source with the new timestamp
			updatedSource := &models.PersonSource{
				URL:         source.URL,
				Title:       source.Title,
				Description: source.Description,
			}

			updatedSources = append(updatedSources, updatedSource)
		}
	}

	// Remove sources no longer present
	if existingPerson != nil && existingPerson.Sources != nil {
		for _, source := range existingPerson.Sources {
			if !sourcesToKeep[source.URL] {
				// Source is no longer associated - remove it
				query := `DELETE FROM person_sources WHERE person_id = $1 AND url = $2`
				_, err := tx.Exec(ctx, query, person.ID, source.URL)
				if err != nil {
					return fmt.Errorf("error removing source: %w", err)
				}
			}
		}
	}

	// Update the person's sources collection
	person.Sources = updatedSources

	return nil
}

func (r *PersonRepository) Delete(ctx context.Context, uuid string) error {
	// Start a transaction
	tx, err := r.container.Database.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
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

	// Delete the person record
	result, err := tx.Exec(ctx, "DELETE FROM people WHERE uuid = $1", uuid)
	if err != nil {
		return fmt.Errorf("error deleting person: %w", err)
	}

	// Check if any rows were affected
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return utils.ErrPersonNotFound
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	// Delete from Elasticsearch after successful deletion
	req := esapi.DeleteRequest{
		Index:      "people",
		DocumentID: uuid,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, r.container.Elastic.Client)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to delete person %s from Elasticsearch", uuid)
		return nil
	}

	// Handle potential close error
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			log.Error().Err(err).Msg("Failed to close Elasticsearch response body")
		}
	}()

	// Check if the Elasticsearch delete request was successful
	if res.IsError() {
		// Only log the error if it's not a 404 (document not found)
		if res.StatusCode != 404 {
			var e map[string]any
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				log.Error().Err(err).Msg("Failed to parse Elasticsearch error response")
			} else {
				log.Error().Err(err).Str("status", res.Status()).Msg("Failed to delete document from Elasticsearch index")
			}
		}
		// Don't return an error since the storage deletion was successful
	}

	return nil
}

func (r *PersonRepository) Search(ctx context.Context, filter models.PersonFilter) (*models.PaginatedPersonResult, error) {
	// Normalize the limit value
	limit := filter.Limit
	if limit <= 0 {
		limit = 50 // default
	} else if limit > 100 {
		limit = 100 // max
	}

	// Build the Elasticsearch query
	query, err := r.prepareSearchQuery(filter, limit)
	if err != nil {
		return nil, fmt.Errorf("error building search query: %w", err)
	}

	// Execute the search
	res, err := r.container.Elastic.Client.Search().Index("people").Request(query).TrackTotalHits(true).Do(ctx)
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
		person, err := r.hitToPerson(hit)
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

func (r *PersonRepository) prepareSearchQuery(filter models.PersonFilter, limit int) (*search.Request, error) {
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
	var searchRequest *search.Request

	switch filter.SortBy {
	case models.PersonSortByCreatorCount:
		// When sorting by creator count, use a function_score with a script
		searchRequest = &search.Request{
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
		searchRequest = &search.Request{
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

		searchRequest = &search.Request{
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
func (r *PersonRepository) hitToPerson(hit types.Hit) (*models.Person, error) {
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
