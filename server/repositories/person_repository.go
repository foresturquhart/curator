package repositories

import (
	"context"
	"errors"
	"fmt"

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

func (r *PersonRepository) getByInternalIDTx(ctx context.Context, tx pgx.Tx, id int64) (*models.Person, error) {
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

func (r *PersonRepository) GetByInternalID(ctx context.Context, id int64) (*models.Person, error) {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
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

	person, err := r.getByInternalIDTx(ctx, tx, id)
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
	tx, err := r.container.Postgres.Pool.Begin(ctx)
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
func (r *PersonRepository) getByNameTx(ctx context.Context, tx pgx.Tx, name string) (*models.Person, error) {
	query := `
        SELECT id, uuid, name, description, created_at, updated_at
        FROM people
        WHERE name = $1
    `

	var person models.Person
	var descriptionPtr *string

	err := tx.QueryRow(ctx, query, name).Scan(
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

// GetAllIDs retrieves all person IDs from the database.
func (r *PersonRepository) GetAllIDs(ctx context.Context) ([]int64, error) {
	rows, err := r.container.Postgres.Pool.Query(ctx, "SELECT id FROM people ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("error querying person IDs: %w", err)
	}
	defer rows.Close()

	var personIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("error scanning person ID: %w", err)
		}
		personIDs = append(personIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating person IDs: %w", err)
	}

	return personIDs, nil
}

// FindImagesByPersonUUID retrieves the image UUIDs associated with a person.
func (r *PersonRepository) FindImagesByPersonUUID(ctx context.Context, personUUID string) ([]int64, error) {
	query := `
        SELECT ip.image_id
        FROM image_people ip
        INNER JOIN people p ON ip.person_id = p.id
        WHERE p.uuid = $1
    `

	rows, err := r.container.Postgres.Pool.Query(ctx, query, personUUID)
	if err != nil {
		return nil, fmt.Errorf("error querying images by person UUID: %w", err)
	}
	defer rows.Close()

	var imageIDs []int64
	for rows.Next() {
		var imageID int64
		if err := rows.Scan(&imageID); err != nil {
			return nil, fmt.Errorf("error scanning image UUID: %w", err)
		}
		imageIDs = append(imageIDs, imageID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating image UUIDs: %w", err)
	}

	return imageIDs, nil
}

// Create inserts a new person record.
func (r *PersonRepository) Create(ctx context.Context, person *models.Person) error {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("Failed to roll back transaction")
			}
		}
	}()

	existingPerson, err := r.getByNameTx(ctx, tx, person.Name)
	if err != nil {
		return fmt.Errorf("error checking for duplicate names: %w", err)
	}

	if existingPerson != nil {
		return &utils.ConflictError{
			Message:      "A person with this name already exists",
			ConflictUUID: existingPerson.UUID,
		}
	}

	query := `
        INSERT INTO people (name, description)
        VALUES ($1, $2)
        RETURNING id, uuid, created_at, updated_at
    `

	err = tx.QueryRow(
		ctx, query,
		person.Name, person.Description,
	).Scan(
		&person.ID, &person.UUID,
		&person.CreatedAt, &person.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("error creating person: %w", err)
	}

	if err := r.syncSourceAssociations(ctx, tx, person, existingPerson); err != nil {
		return fmt.Errorf("error syncing associations: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// Update updates an existing person record.
func (r *PersonRepository) Update(ctx context.Context, person *models.Person) error {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("Failed to roll back transaction")
			}
		}
	}()

	existingPerson, err := r.getByNameTx(ctx, tx, person.Name)
	if err != nil && !errors.Is(err, utils.ErrPersonNotFound) {
		return fmt.Errorf("error checking for duplicate name: %w", err)
	}

	if existingPerson != nil && existingPerson.UUID != person.UUID {
		return &utils.ConflictError{
			Message:      "A person with this name already exists",
			ConflictUUID: existingPerson.UUID,
		}
	}

	if person.ID > 0 {
		existingPerson, err = r.getByInternalIDTx(ctx, tx, person.ID)
	} else {
		existingPerson, err = r.getByUUIDTx(ctx, tx, person.UUID)
	}

	if err != nil {
		return fmt.Errorf("error retrieving person: %w", err)
	}

	query := `
        UPDATE people SET
            name = $1,
            description = $2
        WHERE id = $3
        RETURNING id, uuid, created_at, updated_at
    `

	err = tx.QueryRow(
		ctx, query,
		person.Name, person.Description,
		existingPerson.ID,
	).Scan(
		&person.ID, &person.UUID,
		&person.CreatedAt, &person.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("error updating person: %w", err)
	}

	if err := r.syncSourceAssociations(ctx, tx, person, existingPerson); err != nil {
		return fmt.Errorf("error syncing associations: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
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
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(err).Msg("Failed to roll back transaction")
			}
		}
	}()

	result, err := tx.Exec(ctx, "DELETE FROM people WHERE uuid = $1", uuid)
	if err != nil {
		return fmt.Errorf("error deleting person: %w", err)
	}

	if result.RowsAffected() == 0 {
		return utils.ErrPersonNotFound
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing deletion: %w", err)
	}

	return nil
}
