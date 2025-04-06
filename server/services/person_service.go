package services

import (
	"context"
	"fmt"

	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/search"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/rs/zerolog/log"
)

type PersonService struct {
	container *container.Container
	repo      *repositories.PersonRepository
	search    *search.PersonSearch
}

func NewPersonService(container *container.Container) *PersonService {
	return &PersonService{
		container: container,
		repo:      repositories.NewPersonRepository(container),
		search:    search.NewPersonSearch(container),
	}
}

func (s *PersonService) Get(ctx context.Context, uuid string) (*models.Person, error) {
	return s.repo.GetByUUID(ctx, uuid)
}

func (s *PersonService) GetByInternalID(ctx context.Context, id int64) (*models.Person, error) {
	return s.repo.GetByInternalID(ctx, id)
}

func (s *PersonService) Create(ctx context.Context, person *models.Person) error {
	if err := s.repo.Create(ctx, person); err != nil {
		return fmt.Errorf("failed to create person: %w", err)
	}

	if err := s.search.Index(ctx, person.ToSearchRecord()); err != nil {
		log.Error().Err(err).Msgf("Failed to index person %s", person.UUID)
	}

	return nil
}

func (s *PersonService) Search(ctx context.Context, options *search.PersonSearchOptions) (*utils.PaginatedResult[*models.Person], error) {
	result, err := s.search.Search(ctx, options)

	if err != nil {
		return nil, fmt.Errorf("failed to search for people: %w", err)
	}

	var data []*models.Person
	for _, result := range result.Results {
		data = append(data, result.ToModel())
	}

	return &utils.PaginatedResult[*models.Person]{
		Data:       data,
		HasMore:    result.HasMore,
		TotalCount: result.TotalCount,
		NextCursor: result.NextCursor,
	}, nil
}

func (s *PersonService) Update(ctx context.Context, person *models.Person) error {
	if err := s.repo.Update(ctx, person); err != nil {
		return fmt.Errorf("failed to update person: %w", err)
	}

	if err := s.search.Index(ctx, person.ToSearchRecord()); err != nil {
		log.Error().Err(err).Msgf("Failed to index person %s", person.UUID)
	}

	imageIDs, err := s.repo.FindImagesByPersonUUID(ctx, person.UUID)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to fetch associated images for person %s", person.UUID)
	} else {
		for _, imageID := range imageIDs {
			if err := s.container.Worker.EnqueueReindexImage(ctx, imageID); err != nil {
				log.Error().Err(err).Int64("id", imageID).Msg("Error reindexing image after person deletion")
			}
		}
	}

	return nil
}

func (s *PersonService) Index(ctx context.Context, person *models.Person) error {
	return s.search.Index(ctx, person.ToSearchRecord())
}

func (s *PersonService) IndexAll(ctx context.Context) error {
	// Retrieve all person IDs from the repository.
	personIDs, err := s.repo.GetAllIDs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get person IDs: %w", err)
	}

	// Iterate through IDs and index each person
	for _, id := range personIDs {
		// Get the person by ID
		person, err := s.repo.GetByInternalID(ctx, id)
		if err != nil {
			// Log the error and continue to the next person
			log.Error().Err(err).Msgf("Error retrieving person for id %d", id)
			continue
		}

		// Index in a new transaction
		if err := s.search.Index(ctx, person.ToSearchRecord()); err != nil {
			log.Error().Err(err).Msgf("Error reindexing person %s", person.UUID)
			continue
		}

		log.Info().Msgf("Reindexed person %s", person.UUID)
	}

	return nil
}

func (s *PersonService) Delete(ctx context.Context, uuid string) error {
	imageIDs, err := s.repo.FindImagesByPersonUUID(ctx, uuid)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving associated images for person %s", uuid)
	}

	if err := s.repo.Delete(ctx, uuid); err != nil {
		return err
	}

	// Now trigger external operations
	if err := s.search.Delete(ctx, uuid); err != nil {
		log.Error().Err(err).Msgf("Error deleting person %s from search index", uuid)
	}

	// Handle reindexing for associated images
	for _, imageID := range imageIDs {
		if err := s.container.Worker.EnqueueReindexImage(ctx, imageID); err != nil {
			log.Error().Err(err).Int64("id", imageID).Msg("Error reindexing image after person deletion")
		}
	}

	return nil
}
