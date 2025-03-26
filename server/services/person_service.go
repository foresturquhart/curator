package services

import (
	"context"
	"fmt"

	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/search"
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

func (s *PersonService) Create(ctx context.Context, person *models.Person) error {
	if err := s.repo.Create(ctx, person); err != nil {
		return fmt.Errorf("failed to create person: %w", err)
	}

	if err := s.search.Index(ctx, person); err != nil {
		log.Error().Err(err).Msgf("Failed to index person %s", person.UUID)
	}

	return nil
}

func (s *PersonService) Search(ctx context.Context, filter *models.PersonFilter) (*models.PaginatedPersonResult, error) {
	return s.search.Search(ctx, filter)
}

func (s *PersonService) Update(ctx context.Context, person *models.Person) error {
	if err := s.repo.Update(ctx, person); err != nil {
		return fmt.Errorf("failed to update person: %w", err)
	}

	if err := s.search.Index(ctx, person); err != nil {
		log.Error().Err(err).Msgf("Failed to index person %s", person.UUID)
	}

	imageUUIDs, err := s.repo.FindImagesByPersonUUID(ctx, person.UUID)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to fetch associated images for person %s", person.UUID)
	} else {
		for _, imageUUID := range imageUUIDs {
			if err := s.container.Worker.EnqueueReindexImage(ctx, imageUUID); err != nil {
				log.Error().Err(err).Str("uuid", imageUUID).Msg("Error reindexing image after person deletion")
			}
		}
	}

	return nil
}

func (s *PersonService) Index(ctx context.Context, person *models.Person) error {
	return s.search.Index(ctx, person)
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
		if err := s.search.Index(ctx, person); err != nil {
			log.Error().Err(err).Msgf("Error reindexing person %s", person.UUID)
			continue
		}

		log.Info().Msgf("Reindexed person %s", person.UUID)
	}

	return nil
}

func (s *PersonService) Delete(ctx context.Context, uuid string) error {
	imageUUIDs, err := s.repo.FindImagesByPersonUUID(ctx, uuid)
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
	for _, imageUUID := range imageUUIDs {
		if err := s.container.Worker.EnqueueReindexImage(ctx, imageUUID); err != nil {
			log.Error().Err(err).Str("uuid", imageUUID).Msg("Error reindexing image after person deletion")
		}
	}

	return nil
}
