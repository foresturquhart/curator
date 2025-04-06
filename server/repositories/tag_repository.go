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

type TagRepository struct {
	container *container.Container
}

func NewTagRepository(container *container.Container) *TagRepository {
	return &TagRepository{
		container: container,
	}
}

func (r *TagRepository) getByInternalIDTx(ctx context.Context, tx pgx.Tx, id int64) (*models.Tag, error) {
	query := `
		SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
		FROM tags
		WHERE id = $1
	`

	var tag models.Tag
	var descriptionPtr *string
	var parentIDPtr *int64

	err := tx.QueryRow(ctx, query, id).Scan(
		&tag.ID, &tag.UUID, &tag.Name,
		&descriptionPtr, &parentIDPtr,
		&tag.Position, &tag.CreatedAt, &tag.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrTagNotFound
		}
		return nil, fmt.Errorf("error fetching tag: %w", err)
	}

	tag.Description = descriptionPtr
	tag.ParentID = parentIDPtr

	return &tag, nil
}

func (r *TagRepository) getByUUIDTx(ctx context.Context, tx pgx.Tx, uuid string) (*models.Tag, error) {
	query := `
		SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
		FROM tags
		WHERE uuid = $1
	`

	var tag models.Tag
	var descriptionPtr *string
	var parentIDPtr *int64

	err := tx.QueryRow(ctx, query, uuid).Scan(
		&tag.ID, &tag.UUID, &tag.Name,
		&descriptionPtr, &parentIDPtr,
		&tag.Position, &tag.CreatedAt, &tag.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrTagNotFound
		}
		return nil, fmt.Errorf("error fetching tag: %w", err)
	}

	tag.Description = descriptionPtr
	tag.ParentID = parentIDPtr

	return &tag, nil
}

func (r *TagRepository) GetByInternalID(ctx context.Context, id int64) (*models.Tag, error) {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
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

func (r *TagRepository) GetByUUID(ctx context.Context, uuid string) (*models.Tag, error) {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
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

func (r *TagRepository) GetAllIDs(ctx context.Context) ([]int64, error) {
	rows, err := r.container.Postgres.Pool.Query(ctx, "SELECT id FROM tags ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("error querying tag IDs: %w", err)
	}
	defer rows.Close()

	var tagIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("error scanning tag ID: %w", err)
		}
		tagIDs = append(tagIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tag IDs: %w", err)
	}

	return tagIDs, nil
}

func (r *TagRepository) getByNameTx(ctx context.Context, tx pgx.Tx, name string) (*models.Tag, error) {
	query := `
		SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
        FROM tags
        WHERE name = $1
    `

	var tag models.Tag
	var descriptionPtr *string
	var parentIDPtr *int64

	err := tx.QueryRow(ctx, query, name).Scan(
		&tag.ID, &tag.UUID, &tag.Name,
		&descriptionPtr, &parentIDPtr,
		&tag.Position, &tag.CreatedAt, &tag.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, utils.ErrTagNotFound
		}
		return nil, fmt.Errorf("error fetching tag: %w", err)
	}

	tag.Description = descriptionPtr
	tag.ParentID = parentIDPtr

	return &tag, nil
}

func (r *TagRepository) getAffectedImagesTx(ctx context.Context, tx pgx.Tx, tagID int64) ([]int64, error) {
	var results []int64

	query := `
		WITH RECURSIVE descendants AS (
			SELECT id FROM tags WHERE id = $1
			UNION ALL
			SELECT t.id FROM tags t
			INNER JOIN descendants d ON t.parent_id = d.id
		)
		SELECT DISTINCT image_id FROM image_tags WHERE tag_id IN (SELECT id FROM descendants)
	`

	rows, err := tx.Query(ctx, query, tagID)
	if err != nil {
		return nil, fmt.Errorf("error calculating affected images: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var result int64
		if err := rows.Scan(&result); err != nil {
			return nil, fmt.Errorf("error scanning image id: %w", err)
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over image id rows: %w", err)
	}

	return results, nil
}

type TagHierarchyAction int

const (
	TagHierarchyNone TagHierarchyAction = iota
	TagHierarchyRoot
	TagHierarchyInside
	TagHierarchyBefore
	TagHierarchyAfter
)

type TagUpdateOptions struct {
	Action   TagHierarchyAction
	TargetID *int64
}

func (r *TagRepository) Update(ctx context.Context, tag *models.Tag, opts *TagUpdateOptions) ([]int64, error) {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(rollbackErr).Msg("Failed to roll back transaction")
			}
		}
	}()

	existingTag, err := r.getByNameTx(ctx, tx, tag.Name)
	if err != nil && !errors.Is(err, utils.ErrTagNotFound) {
		return nil, fmt.Errorf("error checking for duplicate name: %w", err)
	}

	if existingTag != nil && existingTag.UUID != tag.UUID {
		return nil, &utils.ConflictError{
			Message:      "A tag with this name already exists",
			ConflictUUID: existingTag.UUID,
		}
	}

	if tag.ID > 0 {
		existingTag, err = r.getByInternalIDTx(ctx, tx, tag.ID)
	} else {
		existingTag, err = r.getByUUIDTx(ctx, tx, tag.UUID)
	}

	if err != nil {
		return nil, fmt.Errorf("error retrieving tag: %w", err)
	}

	query := `
        UPDATE tags SET
            name = $1,
            description = $2
        WHERE id = $3
        RETURNING id, uuid, name, description, parent_id, position, created_at, updated_at
    `

	err = tx.QueryRow(
		ctx, query,
		tag.Name, tag.Description, existingTag.ID,
	).Scan(
		&tag.ID, &tag.UUID, &tag.Name, &tag.Description, &tag.ParentID, &tag.Position, &tag.CreatedAt, &tag.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("error updating tag: %w", err)
	}

	if opts != nil && opts.Action != TagHierarchyNone {
		if opts.Action == TagHierarchyRoot {
			query := `
				SELECT parent_id, position, updated_at
				FROM move_tag_inside($1, NULL)
			`

			err = tx.QueryRow(
				ctx, query,
				existingTag.ID,
			).Scan(
				&tag.ParentID, &tag.Position, &tag.UpdatedAt,
			)

			if err != nil {
				return nil, fmt.Errorf("error moving tag to root: %w", err)
			}
		} else {
			targetTag, err := r.getByInternalIDTx(ctx, tx, *opts.TargetID)
			if err != nil && !errors.Is(err, utils.ErrTagNotFound) {
				return nil, fmt.Errorf("error retrieving target tag: %w", err)
			}

			if opts.Action == TagHierarchyInside && *existingTag.ParentID != targetTag.ID {
				query := `
					SELECT parent_id, position, updated_at
					FROM move_tag_inside($1, $2)
				`

				err = tx.QueryRow(
					ctx, query,
					existingTag.ID, targetTag.ID,
				).Scan(
					&tag.ParentID, &tag.Position, &tag.UpdatedAt,
				)

				if err != nil {
					return nil, fmt.Errorf("error moving tag inside target: %w", err)
				}
			} else if opts.Action == TagHierarchyBefore {
				query := `
					SELECT parent_id, position, updated_at
					FROM move_tag_before($1, $2)
				`

				err = tx.QueryRow(
					ctx, query,
					existingTag.ID, targetTag.ID,
				).Scan(
					&tag.ParentID, &tag.Position, &tag.UpdatedAt,
				)

				if err != nil {
					return nil, fmt.Errorf("error moving tag before target: %w", err)
				}
			} else if opts.Action == TagHierarchyAfter {
				query := `
					SELECT parent_id, position, updated_at
					FROM move_tag_after($1, $2)
				`

				err = tx.QueryRow(
					ctx, query,
					existingTag.ID, targetTag.ID,
				).Scan(
					&tag.ParentID, &tag.Position, &tag.UpdatedAt,
				)

				if err != nil {
					return nil, fmt.Errorf("error moving tag after target: %w", err)
				}
			}
		}
	}

	affectedImages, err := r.getAffectedImagesTx(ctx, tx, existingTag.ID)
	if err != nil {
		return nil, fmt.Errorf("error calculating affected images: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return affectedImages, nil
}

type TagCreateOptions struct {
	Action   TagHierarchyAction
	TargetID *int64
}

func (r *TagRepository) Create(ctx context.Context, tag *models.Tag, opts TagCreateOptions) error {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(rollbackErr).Msg("Failed to roll back transaction")
			}
		}
	}()

	if opts.Action == TagHierarchyRoot {
		query := `
			SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
			FROM insert_tag_inside(NULL, $1, $2)
		`

		err = tx.QueryRow(
			ctx, query,
			tag.Name, tag.Description,
		).Scan(
			&tag.ID, &tag.UUID,
			&tag.Name, &tag.Description,
			&tag.ParentID, &tag.Position,
			&tag.CreatedAt, &tag.UpdatedAt,
		)

		if err != nil {
			return fmt.Errorf("error creating root tag: %w", err)
		}
	} else {
		targetTag, err := r.getByInternalIDTx(ctx, tx, *opts.TargetID)
		if err != nil && !errors.Is(err, utils.ErrTagNotFound) {
			return fmt.Errorf("error retrieving target tag: %w", err)
		}

		if opts.Action == TagHierarchyInside {
			query := `
				SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
				FROM insert_tag_inside($1, $2, $3)
			`

			err = tx.QueryRow(
				ctx, query,
				targetTag.ID,
				tag.Name, tag.Description,
			).Scan(
				&tag.ID, &tag.UUID,
				&tag.Name, &tag.Description,
				&tag.ParentID, &tag.Position,
				&tag.CreatedAt, &tag.UpdatedAt,
			)

			if err != nil {
				return fmt.Errorf("error creating root tag: %w", err)
			}
		} else if opts.Action == TagHierarchyBefore {
			query := `
				SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
				FROM insert_tag_before($1, $2, $3)
			`

			err = tx.QueryRow(
				ctx, query,
				targetTag.ID,
				tag.Name, tag.Description,
			).Scan(
				&tag.ID, &tag.UUID,
				&tag.Name, &tag.Description,
				&tag.ParentID, &tag.Position,
				&tag.CreatedAt, &tag.UpdatedAt,
			)

			if err != nil {
				return fmt.Errorf("error creating root tag: %w", err)
			}
		} else if opts.Action == TagHierarchyAfter {
			query := `
				SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
				FROM insert_tag_after($1, $2, $3)
			`

			err = tx.QueryRow(
				ctx, query,
				targetTag.ID,
				tag.Name, tag.Description,
			).Scan(
				&tag.ID, &tag.UUID,
				&tag.Name, &tag.Description,
				&tag.ParentID, &tag.Position,
				&tag.CreatedAt, &tag.UpdatedAt,
			)

			if err != nil {
				return fmt.Errorf("error creating root tag: %w", err)
			}
		} else {
			return fmt.Errorf("a hierarchy operation must be specified when creating a tag")
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

func (r *TagRepository) Merge(ctx context.Context, sourceTag *models.Tag, destinationTag *models.Tag) ([]int64, error) {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(rollbackErr).Msg("Failed to roll back transaction")
			}
		}
	}()

	if sourceTag.ID <= 0 {
		if sourceTag.UUID != "" {
			sourceTag, err = r.getByUUIDTx(ctx, tx, sourceTag.UUID)
			if err != nil {
				return nil, fmt.Errorf("error retrieving source tag: %w", err)
			}
		} else {
			return nil, fmt.Errorf("source tag is invalid")
		}
	}

	if destinationTag.ID <= 0 {
		if destinationTag.UUID != "" {
			destinationTag, err = r.getByUUIDTx(ctx, tx, destinationTag.UUID)
			if err != nil {
				return nil, fmt.Errorf("error retrieving destination tag: %w", err)
			}
		} else {
			return nil, fmt.Errorf("destination tag is invalid")
		}
	}

	if sourceTag.ID == destinationTag.ID {
		return nil, fmt.Errorf("source tag and destination tag are the same")
	}

	query := `
		SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
		FROM merge_tags($1, $2)
	`

	err = tx.QueryRow(
		ctx, query,
		destinationTag.ID, sourceTag.ID,
	).Scan(
		&destinationTag.ID, &destinationTag.UUID, &destinationTag.Name, &destinationTag.Description, &destinationTag.ParentID, &destinationTag.Position, &destinationTag.CreatedAt, &destinationTag.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("error merging tags: %w", err)
	}

	affectedImages, err := r.getAffectedImagesTx(ctx, tx, destinationTag.ID)
	if err != nil {
		return nil, fmt.Errorf("error calculating affected images: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return affectedImages, nil
}

func (r *TagRepository) Delete(ctx context.Context, tag *models.Tag) ([]int64, error) {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(rollbackErr).Msg("Failed to roll back transaction")
			}
		}
	}()

	if tag.ID <= 0 {
		if tag.UUID != "" {
			tag, err = r.getByUUIDTx(ctx, tx, tag.UUID)
			if err != nil {
				return nil, fmt.Errorf("error retrieving tag: %w", err)
			}
		} else {
			return nil, fmt.Errorf("tag is invalid")
		}
	}

	affectedImages, err := r.getAffectedImagesTx(ctx, tx, tag.ID)
	if err != nil {
		return nil, fmt.Errorf("error calculating affected images: %w", err)
	}

	query := `SELECT delete_tag_recursive($1)`

	_, err = tx.Exec(ctx, query, tag.ID)

	if err != nil {
		return nil, fmt.Errorf("error deleting tag: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}

	return affectedImages, nil
}

// GetChildren fetches the direct children of a tag
func (r *TagRepository) GetChildren(ctx context.Context, parentID *int64) ([]*models.Tag, error) {
	tx, err := r.container.Postgres.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("error starting transaction: %w", err)
	}

	defer func() {
		if tx != nil {
			rollbackErr := tx.Rollback(ctx)
			if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				log.Error().Err(rollbackErr).Msg("Failed to roll back transaction")
			}
		}
	}()

	var query string
	var args []interface{}

	if parentID == nil {
		query = `
			SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
			FROM tags
			WHERE parent_id IS NULL
			ORDER BY position
		`
	} else {
		query = `
			SELECT id, uuid, name, description, parent_id, position, created_at, updated_at
			FROM tags
			WHERE parent_id = $1
			ORDER BY position
		`
		args = append(args, *parentID)
	}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("error querying tag children: %w", err)
	}
	defer rows.Close()

	var tags []*models.Tag
	for rows.Next() {
		var tag models.Tag
		var descriptionPtr *string
		var parentIDPtr *int64

		if err := rows.Scan(
			&tag.ID, &tag.UUID, &tag.Name,
			&descriptionPtr, &parentIDPtr,
			&tag.Position, &tag.CreatedAt, &tag.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("error scanning tag row: %w", err)
		}

		tag.Description = descriptionPtr
		tag.ParentID = parentIDPtr

		tags = append(tags, &tag)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tag rows: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("error committing transaction: %w", err)
	}
	tx = nil

	return tags, nil
}
