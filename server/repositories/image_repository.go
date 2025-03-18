package repositories

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/jackc/pgx/v5"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"github.com/pgvector/pgvector-go"
	"github.com/qdrant/go-client/qdrant"
	"github.com/rs/zerolog/log"
	"time"
)

type ImageRepository struct {
	container *container.Container
}

func NewImageRepository(container *container.Container) *ImageRepository {
	return &ImageRepository{
		container: container,
	}
}

func (r *ImageRepository) InitializeQdrantCollection(ctx context.Context) error {
	exists, err := r.container.Qdrant.GetClient().CollectionExists(ctx, "images")
	if err != nil {
		return fmt.Errorf("error checking for images collection: %w", err)
	}

	if !exists {
		err := r.container.Qdrant.GetClient().CreateCollection(context.Background(), &qdrant.CreateCollection{
			CollectionName: "images",
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     512,
				Distance: qdrant.Distance_Cosine,
			}),
		})

		if err != nil {
			return fmt.Errorf("error initialising images collection: %w", err)
		}
	}

	return nil
}

func (r *ImageRepository) reindexOpenSearch(ctx context.Context, image *models.Image) error {
	// Construct the document to index
	document := map[string]interface{}{
		"id":          image.ID,
		"uuid":        image.UUID,
		"filename":    image.Filename,
		"md5":         image.MD5,
		"sha1":        image.SHA1,
		"width":       image.Width,
		"height":      image.Height,
		"format":      image.Format,
		"size":        image.Size,
		"created_at":  image.CreatedAt,
		"updated_at":  image.UpdatedAt,
		"tags_count":  len(image.Tags),
		"pixel_count": int64(image.Width) * int64(image.Height),
	}

	// Handle nullable fields
	if image.Title != nil {
		document["title"] = *image.Title
	}

	if image.Description != nil {
		document["description"] = *image.Description
	}

	// Add tags
	if len(image.Tags) > 0 {
		tags := make([]map[string]interface{}, len(image.Tags))
		for i, tag := range image.Tags {
			tags[i] = map[string]interface{}{
				"id":       tag.ID,
				"uuid":     tag.UUID,
				"name":     tag.Name,
				"added_at": tag.AddedAt,
			}
		}
		document["tags"] = tags
	}

	// Add people
	if len(image.People) > 0 {
		people := make([]map[string]interface{}, len(image.People))
		for i, person := range image.People {
			people[i] = map[string]interface{}{
				"id":       person.ID,
				"uuid":     person.UUID,
				"name":     person.Name,
				"role":     person.Role,
				"added_at": person.AddedAt,
			}
		}
		document["people"] = people
	}

	// Add sources
	if len(image.Sources) > 0 {
		sources := make([]map[string]interface{}, len(image.Sources))
		for _, source := range image.Sources {
			sourceDoc := map[string]interface{}{
				"url": source.URL,
			}

			// Handle nullable fields
			if source.Title != nil {
				sourceDoc["title"] = *source.Title
			}

			if source.Description != nil {
				sourceDoc["description"] = *source.Description
			}

			sources = append(sources, sourceDoc)
		}
		document["sources"] = sources
	}

	// Encode the document
	payload, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("error encoding document: %w", err)
	}

	// Create index request
	req := opensearchapi.IndexRequest{
		Index:      "images",
		DocumentID: image.UUID,
		Body:       bytes.NewReader(payload),
		// Make the document immediately searchable
		Refresh: "true",
	}

	// Execute the request
	res, err := req.Do(ctx, r.container.OpenSearch.GetClient())
	if err != nil {
		return fmt.Errorf("error executing index request: %w", err)
	}

	// Handle potential close error
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			fmt.Printf("error closing Elasticsearch response body: %v\n", closeErr)
		}
	}()

	// Check if the request was successful
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return fmt.Errorf("error parsing the response body: %w", err)
		}
		return fmt.Errorf("error indexing document [status:%s]: %v", res.Status(), e)
	}

	return nil
}

func (r *ImageRepository) reindexQdrant(ctx context.Context, image *models.Image) error {
	_, err := r.container.Qdrant.GetClient().Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: "images",
		Points: []*qdrant.PointStruct{
			{
				Id:      qdrant.NewIDUUID(image.UUID),
				Vectors: qdrant.NewVectors(image.Embedding.Slice()...),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("error executing upsert: %w", err)
	}

	return nil
}

func (r *ImageRepository) Reindex(ctx context.Context, image *models.Image) error {
	if err := r.reindexOpenSearch(ctx, image); err != nil {
		return fmt.Errorf("error indexing image in opensearch: %w", err)
	}

	if err := r.reindexQdrant(ctx, image); err != nil {
		return fmt.Errorf("error indexing image in qdrant: %w", err)
	}

	return nil
}

func (r *ImageRepository) ReindexAll(ctx context.Context) error {
	tx, err := r.container.Database.GetPool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	// Get all image IDs
	rows, err := tx.Query(ctx, "SELECT id FROM images ORDER BY id")
	if err != nil {
		return fmt.Errorf("error querying image IDs: %w", err)
	}
	defer rows.Close()

	var imageIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("error scanning image ID: %w", err)
		}
		imageIDs = append(imageIDs, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating image IDs: %w", err)
	}

	// Commit the transaction to release the connection
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction to get IDs: %w", err)
	}

	// Iterate through IDs and reindex each image
	for _, id := range imageIDs {
		fmt.Printf("Reindexing image ID %d...\n", id)

		// Get the image by ID
		image, err := r.GetByID(ctx, id)
		if err != nil {
			// Log the error and continue to the next image
			fmt.Printf("Error retrieving image ID %d: %v\n", id, err)
			continue
		}

		// Reindex in a new transaction
		if err := r.Reindex(ctx, image); err != nil {
			fmt.Printf("Error reindexing image ID %d: %v\n", id, err)
			continue
		}
	}

	return nil
}

func (r *ImageRepository) getByIDTx(ctx context.Context, tx pgx.Tx, id int64) (*models.Image, error) {
	query := `
		SELECT id, uuid, filename, md5, sha1, width, height, format, size,
			   embedding, title, description, created_at, updated_at
		FROM images
		WHERE id = $1
	`

	var image models.Image
	var titlePtr, descriptionPtr *string

	err := tx.QueryRow(ctx, query, id).Scan(
		&image.ID, &image.UUID, &image.Filename, &image.MD5, &image.SHA1,
		&image.Width, &image.Height, &image.Format, &image.Size, &image.Embedding,
		&titlePtr, &descriptionPtr, &image.CreatedAt, &image.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrImageNotFound
		}
		return nil, fmt.Errorf("error fetching image: %w", err)
	}

	image.Title = titlePtr
	image.Description = descriptionPtr

	// Fetch all associations
	err = r.fetchImageAssociations(ctx, tx, &image)
	if err != nil {
		return nil, err
	}

	return &image, nil
}

func (r *ImageRepository) GetByID(ctx context.Context, id int64) (*models.Image, error) {
	tx, err := r.container.Database.GetPool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	// Ensure we handle rollback errors
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				fmt.Printf("error rolling back transaction: %v\n", rollbackErr)
			}
		}
	}()

	image, err := r.getByIDTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return image, nil
}

func (r *ImageRepository) getByUUIDTx(ctx context.Context, tx pgx.Tx, uuid string) (*models.Image, error) {
	query := `
		SELECT id, uuid, filename, md5, sha1, width, height, format, size,
			   embedding, title, description, created_at, updated_at
		FROM images
		WHERE uuid = $1
	`

	var image models.Image
	var titlePtr, descriptionPtr *string

	err := tx.QueryRow(ctx, query, uuid).Scan(
		&image.ID, &image.UUID, &image.Filename, &image.MD5, &image.SHA1,
		&image.Width, &image.Height, &image.Format, &image.Size, &image.Embedding,
		&titlePtr, &descriptionPtr, &image.CreatedAt, &image.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrImageNotFound
		}
		return nil, fmt.Errorf("error fetching image: %w", err)
	}

	image.Title = titlePtr
	image.Description = descriptionPtr

	// Fetch all associations
	err = r.fetchImageAssociations(ctx, tx, &image)
	if err != nil {
		return nil, err
	}

	return &image, nil
}

func (r *ImageRepository) GetByUUID(ctx context.Context, uuid string) (*models.Image, error) {
	tx, err := r.container.Database.GetPool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	// Ensure we handle rollback errors
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				fmt.Printf("error rolling back transaction: %v\n", rollbackErr)
			}
		}
	}()

	image, err := r.getByUUIDTx(ctx, tx, uuid)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return image, nil
}

func (r *ImageRepository) Upsert(ctx context.Context, image *models.Image) error {
	// Start a transaction
	tx, err := r.container.Database.GetPool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	// Ensure we handle rollback errors
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				fmt.Printf("error rolling back transaction: %v\n", rollbackErr)
			}
		}
	}()

	// Get the existing image to compare associations
	var existingImage *models.Image

	// Determine if this is an insert or update
	isUpdate := image.ID > 0 || image.UUID != ""

	if isUpdate {
		if image.ID > 0 {
			existingImage, err = r.getByIDTx(ctx, tx, image.ID)
		} else {
			existingImage, err = r.getByUUIDTx(ctx, tx, image.UUID)
		}

		if err != nil {
			return fmt.Errorf("error retrieving existing image: %w", err)
		}

		if existingImage.Filename != image.Filename {
			return fmt.Errorf("image filename is immutable")
		}

		if existingImage.MD5 != image.MD5 || existingImage.SHA1 != image.SHA1 {
			return fmt.Errorf("image hashes are immutable")
		}

		if existingImage.Width != image.Width || existingImage.Height != image.Height {
			return fmt.Errorf("image dimensions are immutable")
		}

		if existingImage.Format != image.Format {
			return fmt.Errorf("image format is immutable")
		}

		if existingImage.Size != image.Size {
			return fmt.Errorf("image size is immutable")
		}

		if existingImage.Embedding != image.Embedding {
			return fmt.Errorf("image embedding is immutable")
		}

		// Perform the update
		query := `
			UPDATE images SET
				title = $1,
				description = $2
			WHERE id = $3
			RETURNING id, uuid, created_at, updated_at
		`

		err = tx.QueryRow(
			ctx, query, image.Title, image.Description, existingImage.ID,
		).Scan(&image.ID, &image.UUID, &image.CreatedAt, &image.UpdatedAt)

		if err != nil {
			return fmt.Errorf("error updating image: %w", err)
		}
	} else {
		// Create new image
		query := `
			INSERT INTO images (
				filename, md5, sha1, width, height, format, size,
				embedding, title, description
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
			) RETURNING id, uuid, created_at, updated_at
		`

		err = tx.QueryRow(ctx, query,
			image.Filename, image.MD5, image.SHA1,
			image.Width, image.Height, image.Format, image.Size,
			image.Embedding, image.Title, image.Description,
		).Scan(&image.ID, &image.UUID, &image.CreatedAt, &image.UpdatedAt)

		if err != nil {
			return fmt.Errorf("error inserting image: %w", err)
		}
	}

	// Synchronise tag associations
	if err := r.syncTagAssociations(ctx, tx, image, existingImage); err != nil {
		return fmt.Errorf("error handling tag associations: %w", err)
	}

	// Synchronise people associations
	if err := r.syncPeopleAssociations(ctx, tx, image, existingImage); err != nil {
		return fmt.Errorf("error handling people associations: %w", err)
	}

	// Synchronise source associations
	if err := r.syncSourceAssociations(ctx, tx, image, existingImage); err != nil {
		return fmt.Errorf("error handling source associations: %w", err)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	// Index in Elasticsearch after successful storage commit
	if err := r.Reindex(ctx, image); err != nil {
		// Log the error but don't fail the operation
		fmt.Printf("error indexing image %s in Elasticsearch: %v\n", image.UUID, err)
	}

	return nil
}

// syncTagAssociations synchronises tag associations for an image
func (r *ImageRepository) syncTagAssociations(ctx context.Context, tx pgx.Tx, image *models.Image, existingImage *models.Image) error {
	// Create maps to track existing and new tags
	existingTags := make(map[string]*models.ImageTag)
	if existingImage != nil {
		for _, tag := range existingImage.Tags {
			existingTags[tag.UUID] = tag
		}
	}

	// Map to track tags we need to retain
	tagsToKeep := make(map[string]bool)

	// Process each tag in the input model
	updatedTags := make([]*models.ImageTag, 0, len(image.Tags))

	for _, tag := range image.Tags {
		if tag == nil {
			continue
		}

		// Determine if we need to look up by UUID or name
		var findQuery string
		var findParam interface{}

		if tag.UUID != "" {
			findQuery = `SELECT id, uuid, name FROM tags WHERE uuid = $1`
			findParam = tag.UUID
		} else if tag.ID > 0 {
			findQuery = `SELECT id, uuid, name FROM tags WHERE id = $1`
			findParam = tag.ID
		} else if tag.Name != "" {
			findQuery = `SELECT id, uuid, name FROM tags WHERE LOWER(name) = LOWER($1)`
			findParam = tag.Name
		} else {
			// If not ID nor UUID nor name are provided, skip this tag
			continue
		}

		// Try to find the existing tag
		var tagID int64
		var tagUUID, tagName string
		err := tx.QueryRow(ctx, findQuery, findParam).Scan(&tagID, &tagUUID, &tagName)

		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Tag doesn't exist - return an error
				return fmt.Errorf("tag with identifier %v does not exist", findParam)
			}
			return fmt.Errorf("error finding tag: %w", err)
		}

		// Create an updated tag object with complete information
		updatedTag := &models.ImageTag{
			ID:   tagID,
			UUID: tagUUID,
			Name: tagName,
		}

		// Mark this tag as one to keep
		tagsToKeep[tagUUID] = true

		// Check if this tag is already associated
		if existingTag, exists := existingTags[tagUUID]; exists {
			// Tag already exists - keep the original added_at time
			updatedTag.AddedAt = existingTag.AddedAt
		} else {
			// New association - create it
			query := `
				INSERT INTO image_tags (image_id, tag_id)
				VALUES ($1, $2)
				RETURNING created_at
			`

			err = tx.QueryRow(ctx, query, image.ID, tagID).Scan(&updatedTag.AddedAt)
			if err != nil {
				return fmt.Errorf("error associating tag: %w", err)
			}
		}

		updatedTags = append(updatedTags, updatedTag)
	}

	// Remove associations for tags no longer present
	if existingImage != nil {
		for _, tag := range existingImage.Tags {
			if !tagsToKeep[tag.UUID] {
				// Tag is no longer associated - remove it
				query := `DELETE FROM image_tags WHERE image_id = $1 AND tag_id = $2`
				_, err := tx.Exec(ctx, query, image.ID, tag.ID)
				if err != nil {
					return fmt.Errorf("error removing tag association: %w", err)
				}
			}
		}
	}

	// Update the image's tags collection
	image.Tags = updatedTags

	return nil
}

// syncPeopleAssociations synchronises people associations for an image
func (r *ImageRepository) syncPeopleAssociations(ctx context.Context, tx pgx.Tx, image *models.Image, existingImage *models.Image) error {
	// Create maps to track existing people
	existingPeople := make(map[string]*models.ImagePerson)
	if existingImage != nil && existingImage.People != nil {
		for _, person := range existingImage.People {
			// Use combination of UUID and role as key
			key := fmt.Sprintf("%s:%s", person.UUID, person.Role)
			existingPeople[key] = person
		}
	}

	// Map to track people we need to retain
	peopleToKeep := make(map[string]bool)

	// Process each person in the input model
	updatedPeople := make([]*models.ImagePerson, 0, len(image.People))

	for _, person := range image.People {
		if person == nil {
			continue
		}

		// Determine if we need to look up by UUID or ID
		var findQuery string
		var findParam interface{}

		if person.UUID != "" {
			findQuery = `SELECT id, uuid, name FROM people WHERE uuid = $1`
			findParam = person.UUID
		} else if person.ID > 0 {
			findQuery = `SELECT id, uuid, name FROM people WHERE id = $1`
			findParam = person.ID
		} else {
			// Neither UUID nor ID is provided
			continue
		}

		// Try to find the existing person
		var personID int64
		var personUUID, personName string
		err := tx.QueryRow(ctx, findQuery, findParam).Scan(&personID, &personUUID, &personName)

		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// Person doesn't exist - return an error
				return fmt.Errorf("person with identifier %v does not exist", findParam)
			}
			return fmt.Errorf("error finding person: %w", err)
		}

		// Ensure we have a valid role
		if person.Role == "" || (person.Role != models.RoleCreator && person.Role != models.RoleSubject) {
			return fmt.Errorf("person %s must have a valid role specified", personName)
		}

		// Create an updated person object with complete information
		updatedPerson := &models.ImagePerson{
			ID:   personID,
			UUID: personUUID,
			Name: personName,
			Role: person.Role,
		}

		// Generate a unique key that includes the role
		uniqueKey := fmt.Sprintf("%s:%s", personUUID, person.Role)
		peopleToKeep[uniqueKey] = true

		// Use the ON CONFLICT trick to either insert a new association or get the existing one's timestamp
		query := `
            INSERT INTO image_people (image_id, person_id, role)
            VALUES ($1, $2, $3)
            ON CONFLICT (image_id, person_id, role) DO UPDATE
            SET role = EXCLUDED.role
            RETURNING created_at
        `

		err = tx.QueryRow(ctx, query, image.ID, personID, person.Role).Scan(&updatedPerson.AddedAt)
		if err != nil {
			return fmt.Errorf("error upserting person association: %w", err)
		}

		updatedPeople = append(updatedPeople, updatedPerson)
	}

	// Remove associations for people no longer present
	if existingImage != nil && existingImage.People != nil {
		for _, person := range existingImage.People {
			uniqueKey := fmt.Sprintf("%s:%s", person.UUID, person.Role)
			if !peopleToKeep[uniqueKey] {
				// Person+role is no longer associated - remove it
				query := `
                    DELETE FROM image_people 
                    WHERE image_id = $1 AND person_id = $2 AND role = $3
                `
				_, err := tx.Exec(ctx, query, image.ID, person.ID, person.Role)
				if err != nil {
					return fmt.Errorf("error removing person association: %w", err)
				}
			}
		}
	}

	// Update the image's people collection
	image.People = updatedPeople

	return nil
}

// syncSourceAssociations synchronises source associations for an image
func (r *ImageRepository) syncSourceAssociations(ctx context.Context, tx pgx.Tx, image *models.Image, existingImage *models.Image) error {
	// Create map to track existing sources
	existingSourcesByURL := make(map[string]*models.ImageSource)

	if existingImage != nil && existingImage.Sources != nil {
		for _, source := range existingImage.Sources {
			existingSourcesByURL[source.URL] = source
		}
	}

	// Map to track sources we need to retain
	sourcesToKeep := make(map[string]bool)

	// Process each source in the input model
	updatedSources := make([]*models.ImageSource, 0, len(image.Sources))

	for _, source := range image.Sources {
		if source == nil || source.URL == "" {
			continue
		}

		// Mark this source URL as one to keep
		sourcesToKeep[source.URL] = true

		// Check if this source URL is already associated with this image
		if _, exists := existingSourcesByURL[source.URL]; exists {
			// Source already exists - update its information
			query := `
                UPDATE image_sources 
                SET title = $1, description = $2
                WHERE image_id = $3 AND url = $4
			`

			_, err := tx.Exec(ctx, query, source.Title, source.Description, image.ID, source.URL)
			if err != nil {
				return fmt.Errorf("error updating source: %w", err)
			}

			// Create updated source with the original timestamp
			updatedSource := &models.ImageSource{
				URL:         source.URL,
				Title:       source.Title,
				Description: source.Description,
			}

			updatedSources = append(updatedSources, updatedSource)
		} else {
			// New source - insert it
			query := `
                INSERT INTO image_sources (image_id, url, title, description)
                VALUES ($1, $2, $3, $4)
            `

			_, err := tx.Exec(ctx, query, image.ID, source.URL, source.Title, source.Description)
			if err != nil {
				return fmt.Errorf("error creating source: %w", err)
			}

			// Create updated source with the new timestamp
			updatedSource := &models.ImageSource{
				URL:         source.URL,
				Title:       source.Title,
				Description: source.Description,
			}

			updatedSources = append(updatedSources, updatedSource)
		}
	}

	// Remove sources no longer present
	if existingImage != nil && existingImage.Sources != nil {
		for _, source := range existingImage.Sources {
			if !sourcesToKeep[source.URL] {
				// Source is no longer associated - remove it
				query := `DELETE FROM image_sources WHERE image_id = $1 AND url = $2`
				_, err := tx.Exec(ctx, query, image.ID, source.URL)
				if err != nil {
					return fmt.Errorf("error removing source: %w", err)
				}
			}
		}
	}

	// Update the image's sources collection
	image.Sources = updatedSources

	return nil
}

func (r *ImageRepository) Delete(ctx context.Context, uuid string) error {
	// Start a transaction
	tx, err := r.container.Database.GetPool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	// Ensure we handle rollback errors
	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				// Just log the rollback error as there's not much we can do at this point
				fmt.Printf("error rolling back transaction: %v\n", rollbackErr)
			}
		}
	}()

	// Delete the image record
	result, err := tx.Exec(ctx, "DELETE FROM images WHERE uuid = $1", uuid)
	if err != nil {
		return fmt.Errorf("error deleting image: %w", err)
	}

	// Check if any rows were affected
	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return utils.ErrImageNotFound
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	// Delete from Elasticsearch after successful deletion
	req := opensearchapi.DeleteRequest{
		Index:      "images",
		DocumentID: uuid,
		Refresh:    "true",
	}

	res, err := req.Do(ctx, r.container.OpenSearch.GetClient())
	if err != nil {
		fmt.Printf("error deleting image %s from Elasticsearch: %v\n", uuid, err)
		return nil
	}

	// Handle potential close error
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			fmt.Printf("error closing Elasticsearch response body: %v\n", closeErr)
		}
	}()

	// Check if the Elasticsearch delete request was successful
	if res.IsError() {
		// Only log the error if it's not a 404 (document not found)
		if res.StatusCode != 404 {
			var e map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
				fmt.Printf("error parsing the Elasticsearch error response: %v\n", err)
			} else {
				fmt.Printf("error deleting document from Elasticsearch [status:%s]: %v\n", res.Status(), e)
			}
		}
		// Don't return an error since the storage deletion was successful
	}

	// Delete from Qdrant after successful deletion
	_, err = r.container.Qdrant.GetClient().Delete(ctx, &qdrant.DeletePoints{
		CollectionName: "images",
		Points:         qdrant.NewPointsSelector(qdrant.NewIDUUID(uuid)),
	})

	if err != nil {
		fmt.Printf("error deleting image %s from Qdrant: %v\n", uuid, err)
		return nil
	}

	return nil
}

func (r *ImageRepository) Search(ctx context.Context, filter models.ImageFilter) (*models.PaginatedImageResult, error) {
	// Normalize the limit value
	limit := filter.Limit
	if limit <= 0 {
		limit = 50 // default
	} else if limit > 100 {
		limit = 100 // max
	}

	// Build the Elasticsearch query
	query, err := r.prepareSearchQuery(ctx, filter, limit)
	if err != nil {
		return nil, fmt.Errorf("error building search query: %w", err)
	}

	// Convert to JSON
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(query); err != nil {
		return nil, fmt.Errorf("error encoding query: %w", err)
	}

	//fmt.Println(string(buf.Bytes()))

	// Execute the search
	res, err := r.container.OpenSearch.GetClient().Search(
		r.container.OpenSearch.GetClient().Search.WithContext(ctx),
		r.container.OpenSearch.GetClient().Search.WithIndex("images"),
		r.container.OpenSearch.GetClient().Search.WithBody(&buf),
		r.container.OpenSearch.GetClient().Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return nil, fmt.Errorf("error executing search: %w", err)
	}

	// Handle potential close error
	defer func() {
		if closeErr := res.Body.Close(); closeErr != nil {
			fmt.Printf("error closing Elasticsearch response body: %v\n", closeErr)
		}
	}()

	// Check for errors
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return nil, fmt.Errorf("error parsing the response body: %w", err)
		}
		return nil, fmt.Errorf("error searching documents [status:%s]: %v", res.Status(), e)
	}

	// Parse the response
	var osr map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&osr); err != nil {
		return nil, fmt.Errorf("error parsing the response body: %w", err)
	}

	// Extract total count
	totalHits := int(osr["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))

	// Get the hits
	hits, ok := osr["hits"].(map[string]interface{})["hits"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format")
	}

	// Determine if we have more results by checking if we have one extra hit
	hasMore := len(hits) > limit
	if hasMore {
		hits = hits[:limit] // Remove the extra hit from the data set
	}

	// Convert hits to models
	images := make([]*models.Image, 0, len(hits))
	var nextCursor []interface{}
	for i, hitRaw := range hits {
		hit, ok := hitRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected hit format")
		}
		image, err := r.hitToImage(hit)
		if err != nil {
			return nil, fmt.Errorf("error converting hit to image: %w", err)
		}
		images = append(images, image)

		// If this is the last hit and there are more results, use its "sort" field as the cursor.
		if i == len(hits)-1 && hasMore {
			if sortValues, found := hit["sort"].([]interface{}); found {
				nextCursor = sortValues
			} else {
				return nil, fmt.Errorf("missing sort field in hit for pagination")
			}
		}
	}

	// Use the pagination helper to format the response
	return &models.PaginatedImageResult{
		Data:       images,
		HasMore:    hasMore,
		TotalCount: totalHits,
		NextCursor: nextCursor,
	}, nil
}

func (r *ImageRepository) prepareSearchQuery(ctx context.Context, filter models.ImageFilter, limit int) (map[string]interface{}, error) {
	// Initialize the query structure
	boolQuery := map[string]interface{}{
		"must":     []interface{}{},
		"must_not": []interface{}{},
		"should":   []interface{}{},
	}

	// Initialize the functions
	var functions []interface{}

	// Flag to track if we should return zero results due to no vector matches
	returnEmptyResults := false

	if filter.SimilarToEmbedding != nil || filter.SimilarToID != "" {
		var vectorToSearch []float32

		// Get the vector either directly or by ID
		if filter.SimilarToEmbedding != nil {
			vectorToSearch = filter.SimilarToEmbedding.Slice()
		} else if filter.SimilarToID != "" {
			// Fetch the image to get its embedding
			image, err := r.GetByUUID(ctx, filter.SimilarToID)
			if err != nil {
				return nil, fmt.Errorf("error retrieving reference image: %w", err)
			}
			vectorToSearch = image.Embedding.Slice()
		}

		// Query Qdrant for similar vectors
		searchResults, err := r.container.Qdrant.GetClient().Query(context.Background(), &qdrant.QueryPoints{
			CollectionName: "images",
			Query:          qdrant.NewQuery(vectorToSearch...),
			WithPayload:    qdrant.NewWithPayloadEnable(false),
		})

		if err != nil {
			return nil, fmt.Errorf("error executing vector search: %w", err)
		}

		// Check if we have any results
		if len(searchResults) == 0 {
			// No vector matches, so the final result should be empty
			returnEmptyResults = true
		} else {
			uuids := make([]string, 0, len(searchResults))
			scoresByUUID := make(map[string]float32)

			for _, result := range searchResults {
				uuid := result.Id.GetUuid()
				uuids = append(uuids, uuid)
				scoresByUUID[uuid] = result.Score
			}

			// Add a terms query to filter by these UUIDs
			boolQuery["must"] = append(boolQuery["must"].([]interface{}), map[string]interface{}{
				"terms": map[string]interface{}{
					"uuid": uuids,
				},
			})

			// Use a function score to preserve the vector similarity scores
			for uuid, score := range scoresByUUID {
				functions = append(functions, map[string]interface{}{
					"filter": map[string]interface{}{
						"term": map[string]interface{}{
							"uuid": uuid,
						},
					},
					"weight": score,
				})
			}
		}

		// Set sort by _score by default when doing similarity search
		if filter.SortBy == "" {
			filter.SortBy = models.SortByRelevance
			filter.SortDirection = models.SortDirectionDesc
		}
	}

	// If we should return empty results, use a filter that will never match
	if returnEmptyResults {
		// This is a pattern to intentionally return no results
		boolQuery["must"] = append(boolQuery["must"].([]interface{}), map[string]interface{}{
			"term": map[string]interface{}{
				"uuid": "impossible_uuid_that_will_never_match",
			},
		})
	}

	// Apply title filter
	if filter.Title != "" {
		boolQuery["should"] = append(boolQuery["should"].([]interface{}), map[string]interface{}{
			"match": map[string]interface{}{
				"title": filter.Title,
			},
		})
	}

	// Apply description filter
	if filter.Description != "" {
		boolQuery["should"] = append(boolQuery["should"].([]interface{}), map[string]interface{}{
			"match": map[string]interface{}{
				"description": filter.Description,
			},
		})
	}

	// Apply hash filter
	if filter.Hash != "" {
		boolQuery["must"] = append(boolQuery["must"].([]interface{}), map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []interface{}{
					map[string]interface{}{
						"term": map[string]interface{}{
							"md5": filter.Hash,
						},
					},
					map[string]interface{}{
						"term": map[string]interface{}{
							"sha1": filter.Hash,
						},
					},
				},
				"minimum_should_match": 1,
			},
		})
	}

	// Apply width filters
	if filter.MinWidth > 0 || filter.MaxWidth > 0 {
		widthRange := map[string]interface{}{}
		if filter.MinWidth > 0 {
			widthRange["gte"] = filter.MinWidth
		}
		if filter.MaxWidth > 0 {
			widthRange["lte"] = filter.MaxWidth
		}

		boolQuery["must"] = append(boolQuery["must"].([]interface{}), map[string]interface{}{
			"range": map[string]interface{}{
				"width": widthRange,
			},
		})
	}

	// Apply height filters
	if filter.MinHeight > 0 || filter.MaxHeight > 0 {
		heightRange := map[string]interface{}{}

		if filter.MinHeight > 0 {
			heightRange["gte"] = filter.MinHeight
		}
		if filter.MaxHeight > 0 {
			heightRange["lte"] = filter.MaxHeight
		}

		boolQuery["must"] = append(boolQuery["must"].([]interface{}), map[string]interface{}{
			"range": map[string]interface{}{
				"height": heightRange,
			},
		})
	}

	// Apply date filters
	if filter.SinceDate != nil || filter.BeforeDate != nil {
		dateRange := map[string]interface{}{}

		if filter.SinceDate != nil {
			dateRange["gte"] = filter.SinceDate.Format(time.RFC3339)
		}
		if filter.BeforeDate != nil {
			dateRange["lte"] = filter.BeforeDate.Format(time.RFC3339)
		}

		boolQuery["must"] = append(boolQuery["must"].([]interface{}), map[string]interface{}{
			"range": map[string]interface{}{
				"created_at": dateRange,
			},
		})
	}

	// Apply tag filters
	if len(filter.TagFilters) > 0 {
		for _, tagFilter := range filter.TagFilters {
			tagQuery := map[string]interface{}{
				"nested": map[string]interface{}{
					"path": "tags",
					"query": map[string]interface{}{
						"term": map[string]interface{}{
							"tags.uuid": tagFilter.ID,
						},
					},
				},
			}

			if tagFilter.Include {
				boolQuery["must"] = append(boolQuery["must"].([]interface{}), tagQuery)
			} else {
				boolQuery["must_not"] = append(boolQuery["must_not"].([]interface{}), tagQuery)
			}
		}
	}

	// Apply person filters
	if len(filter.PersonFilters) > 0 {
		for _, personFilter := range filter.PersonFilters {
			personQuery := map[string]interface{}{
				"nested": map[string]interface{}{
					"path": "people",
					"query": map[string]interface{}{
						"bool": map[string]interface{}{
							"must": []interface{}{
								map[string]interface{}{
									"term": map[string]interface{}{
										"people.uuid": personFilter.ID,
									},
								},
								map[string]interface{}{
									"term": map[string]interface{}{
										"people.role": personFilter.Role,
									},
								},
							},
						},
					},
				},
			}

			if personFilter.Include {
				boolQuery["must"] = append(boolQuery["must"].([]interface{}), personQuery)
			} else {
				boolQuery["must_not"] = append(boolQuery["must_not"].([]interface{}), personQuery)
			}
		}
	}

	// Apply source filters
	if len(filter.Sources) > 0 {
		boolQuery["must"] = append(boolQuery["must"].([]interface{}), map[string]interface{}{
			"nested": map[string]interface{}{
				"path": "sources",
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"should": []interface{}{
							map[string]interface{}{
								"terms": map[string]interface{}{
									"sources.url": filter.Sources,
								},
							},
						},
					},
				},
			},
		})
	}

	// Build the base query
	query := map[string]interface{}{
		"size": limit + 1, // Extra document to detect more pages
	}

	// Apply minimum score
	minScore := float32(1)
	if filter.SimilarityThreshold > 0 {
		minScore = filter.SimilarityThreshold
	}
	query["min_score"] = minScore

	// Determine sort field & direction with defaults
	sortField := models.SortByCreatedAt
	if filter.SortBy != "" {
		sortField = filter.SortBy
	} else if filter.SimilarToID != "" || filter.SimilarToEmbedding != nil || filter.Title != "" || filter.Description != "" {
		sortField = models.SortByRelevance
	}
	sortDirection := models.SortDirectionDesc
	if filter.SortDirection != "" {
		sortDirection = filter.SortDirection
	}

	if sortField == models.SortByRandom {
		if filter.RandomSeed != nil {
			functions = []interface{}{
				map[string]interface{}{
					"random_score": map[string]interface{}{
						"seed": *filter.RandomSeed,
					},
				},
			}
		} else {
			return nil, fmt.Errorf("invalid random sorting seed provided")
		}
	} else {
		query["sort"] = []interface{}{
			map[string]interface{}{
				string(sortField): map[string]interface{}{
					"order": sortDirection,
				},
			},
			map[string]interface{}{
				"id": map[string]interface{}{
					"order": "asc",
				},
			},
		}
	}

	query["query"] = map[string]interface{}{
		"function_score": map[string]interface{}{
			"query": map[string]interface{}{
				"bool": boolQuery,
			},
			"boost_mode": "multiply",
			"functions":  functions,
		},
	}

	// If a StartingAfter cursor is provided, attach it
	if filter.StartingAfter != nil {
		query["search_after"] = filter.StartingAfter
	}

	return query, nil
}

// hitToImage converts an Elasticsearch hit to an Image model
func (r *ImageRepository) hitToImage(hit map[string]interface{}) (*models.Image, error) {
	log.Info().Interface("score", hit["_score"]).Interface("uuid", hit["_id"]).Msg("Parsing OpenSearch hit")

	// Ensure _source exists and is valid.
	sourceRaw, ok := hit["_source"]
	if !ok {
		return nil, fmt.Errorf("missing _source field")
	}
	source, ok := sourceRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("_source is not a map")
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

	filename, err := getString("filename")
	if err != nil {
		return nil, err
	}

	md5, err := getString("md5")
	if err != nil {
		return nil, err
	}

	sha1, err := getString("sha1")
	if err != nil {
		return nil, err
	}

	widthFloat, err := getFloat64("width")
	if err != nil {
		return nil, err
	}

	heightFloat, err := getFloat64("height")
	if err != nil {
		return nil, err
	}

	format, err := getString("format")
	if err != nil {
		return nil, err
	}

	sizeFloat, err := getFloat64("size")
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

	// Build the base image.
	image := &models.Image{
		ID:        int64(idFloat),
		UUID:      uuid,
		Filename:  filename,
		MD5:       md5,
		SHA1:      sha1,
		Width:     int(widthFloat),
		Height:    int(heightFloat),
		Format:    models.ImageFormat(format),
		Size:      int64(sizeFloat),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	// Nullable fields.
	if title, err := getString("title"); err == nil {
		image.Title = &title
	}
	if desc, err := getString("description"); err == nil {
		image.Description = &desc
	}

	// Process embedding if available.
	if embRaw, exists := source["embedding"]; exists && embRaw != nil {
		embArr, ok := embRaw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("embedding is not an array")
		}
		embeddingData := make([]float32, len(embArr))
		for i, v := range embArr {
			num, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("embedding element is not a float64")
			}
			embeddingData[i] = float32(num)
		}
		vec := pgvector.NewVector(embeddingData)
		image.Embedding = &vec
	}

	// Process tags.
	if rawTags, exists := source["tags"]; exists && rawTags != nil {
		tagsArr, ok := rawTags.([]interface{})
		if ok {
			tags := make([]*models.ImageTag, 0, len(tagsArr))
			for _, rawTag := range tagsArr {
				tagMap, ok := rawTag.(map[string]interface{})
				if !ok {
					continue
				}
				addedAtStr, ok := tagMap["added_at"].(string)
				if !ok {
					return nil, fmt.Errorf("tag added_at is not a string")
				}
				addedAt, err := time.Parse(time.RFC3339, addedAtStr)
				if err != nil {
					return nil, fmt.Errorf("error parsing tag added_at: %w", err)
				}
				idFloat, ok := tagMap["id"].(float64)
				if !ok {
					return nil, fmt.Errorf("tag id is not a float64")
				}
				tagUUID, ok := tagMap["uuid"].(string)
				if !ok {
					return nil, fmt.Errorf("tag uuid is not a string")
				}
				tagName, ok := tagMap["name"].(string)
				if !ok {
					return nil, fmt.Errorf("tag name is not a string")
				}
				tags = append(tags, &models.ImageTag{
					ID:      int64(idFloat),
					UUID:    tagUUID,
					Name:    tagName,
					AddedAt: addedAt,
				})
			}
			image.Tags = tags
		}
	}

	// Process people.
	if rawPeople, exists := source["people"]; exists && rawPeople != nil {
		peopleArr, ok := rawPeople.([]interface{})
		if ok {
			people := make([]*models.ImagePerson, 0, len(peopleArr))
			for _, rawPerson := range peopleArr {
				personMap, ok := rawPerson.(map[string]interface{})
				if !ok {
					continue
				}
				addedAtStr, ok := personMap["added_at"].(string)
				if !ok {
					return nil, fmt.Errorf("person added_at is not a string")
				}
				addedAt, err := time.Parse(time.RFC3339, addedAtStr)
				if err != nil {
					return nil, fmt.Errorf("error parsing person added_at: %w", err)
				}
				idFloat, ok := personMap["id"].(float64)
				if !ok {
					return nil, fmt.Errorf("person id is not a float64")
				}
				personUUID, ok := personMap["uuid"].(string)
				if !ok {
					return nil, fmt.Errorf("person uuid is not a string")
				}
				name, ok := personMap["name"].(string)
				if !ok {
					return nil, fmt.Errorf("person name is not a string")
				}
				role, ok := personMap["role"].(string)
				if !ok {
					return nil, fmt.Errorf("person role is not a string")
				}
				people = append(people, &models.ImagePerson{
					ID:      int64(idFloat),
					UUID:    personUUID,
					Name:    name,
					Role:    models.PersonRole(role),
					AddedAt: addedAt,
				})
			}
			image.People = people
		}
	}

	// Process sources.
	if rawSources, exists := source["sources"]; exists && rawSources != nil {
		sourcesArr, ok := rawSources.([]interface{})
		if ok {
			sources := make([]*models.ImageSource, 0, len(sourcesArr))
			for _, rawSrc := range sourcesArr {
				if rawSrc == nil {
					continue
				}
				srcMap, ok := rawSrc.(map[string]interface{})
				if !ok {
					continue
				}
				url, ok := srcMap["url"].(string)
				if !ok {
					return nil, fmt.Errorf("source url is not a string")
				}
				imageSource := &models.ImageSource{URL: url}
				if t, ok := srcMap["title"].(string); ok {
					imageSource.Title = &t
				}
				if d, ok := srcMap["description"].(string); ok {
					imageSource.Description = &d
				}
				sources = append(sources, imageSource)
			}
			image.Sources = sources
		}
	}

	return image, nil
}

// fetchImageAssociations populates an image with its associated tags, people, and sources
func (r *ImageRepository) fetchImageAssociations(ctx context.Context, tx pgx.Tx, image *models.Image) error {
	var err error

	// Fetch tags for the image
	image.Tags, err = r.fetchImageTags(ctx, tx, image.ID)
	if err != nil {
		return fmt.Errorf("error fetching image tags: %w", err)
	}

	// Fetch people for the image
	image.People, err = r.fetchImagePeople(ctx, tx, image.ID)
	if err != nil {
		return fmt.Errorf("error fetching image people: %w", err)
	}

	// Fetch sources for the image
	image.Sources, err = r.fetchImageSources(ctx, tx, image.ID)
	if err != nil {
		return fmt.Errorf("error fetching image sources: %w", err)
	}

	return nil
}

// fetchImageTags retrieves all tags associated with an image, including canonical relationships
func (r *ImageRepository) fetchImageTags(ctx context.Context, tx pgx.Tx, imageID int64) ([]*models.ImageTag, error) {
	query := `
		WITH RECURSIVE canonical_chain AS (
			SELECT 
				t.id, 
				t.uuid,
				t.canonical_id, 
				t.name, 
				t.id AS original_tag_id,
				it.created_at AS added_at,
				1 AS depth
			FROM image_tags it
			JOIN tags t ON it.tag_id = t.id
			WHERE it.image_id = $1 
			UNION ALL
			SELECT 
				t.id, 
				t.uuid,
				t.canonical_id, 
				t.name, 
				cc.original_tag_id,
				cc.added_at,
				cc.depth + 1
			FROM canonical_chain cc
			JOIN tags t ON cc.canonical_id = t.id
			WHERE cc.canonical_id IS NOT NULL
			AND cc.depth < 16
		)
		SELECT 
			COALESCE(leaf.id, cc.id) AS tag_id,
			COALESCE(leaf.uuid, cc.uuid) AS tag_uuid,
			COALESCE(leaf.name, cc.name) AS tag_name,
			cc.added_at
		FROM canonical_chain cc
		LEFT JOIN (
			SELECT DISTINCT ON (original_tag_id) 
				original_tag_id, id, uuid, name
			FROM canonical_chain 
			WHERE canonical_id IS NULL
			ORDER BY original_tag_id, depth DESC
		) leaf ON cc.original_tag_id = leaf.original_tag_id
		WHERE cc.depth = 1
		ORDER BY tag_name;
	`

	rows, err := tx.Query(ctx, query, imageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tags []*models.ImageTag
	for rows.Next() {
		var tag models.ImageTag
		err := rows.Scan(&tag.ID, &tag.UUID, &tag.Name, &tag.AddedAt)
		if err != nil {
			return nil, err
		}

		tags = append(tags, &tag)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tags, nil
}

// fetchImagePeople retrieves all people associated with an image, including canonical relationships
func (r *ImageRepository) fetchImagePeople(ctx context.Context, tx pgx.Tx, imageID int64) ([]*models.ImagePerson, error) {
	query := `
		SELECT 
			p.id,
			p.uuid,
			p.name,
			ip.role,
			ip.created_at AS added_at
		FROM image_people ip
		JOIN people p ON ip.person_id = p.id
		WHERE ip.image_id = $1
		ORDER BY p.name, ip.role;
	`

	rows, err := tx.Query(ctx, query, imageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var people []*models.ImagePerson
	for rows.Next() {
		var person models.ImagePerson
		err := rows.Scan(&person.ID, &person.UUID, &person.Name, &person.Role, &person.AddedAt)
		if err != nil {
			return nil, err
		}

		people = append(people, &person)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return people, nil
}

// fetchImageSources retrieves all sources associated with an image
func (r *ImageRepository) fetchImageSources(ctx context.Context, tx pgx.Tx, imageID int64) ([]*models.ImageSource, error) {
	query := `
		SELECT 
			s.url,
			s.title,
			s.description
		FROM image_sources s
		WHERE s.image_id = $1
		ORDER BY s.title, s.url;
	`

	rows, err := tx.Query(ctx, query, imageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []*models.ImageSource
	for rows.Next() {
		var source models.ImageSource
		err := rows.Scan(&source.URL, &source.Title, &source.Description)
		if err != nil {
			return nil, err
		}

		sources = append(sources, &source)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return sources, nil
}
