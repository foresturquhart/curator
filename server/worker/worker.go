package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/services"
	"github.com/foresturquhart/curator/server/tasks"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Worker represents the background job processor
type Worker struct {
	server *asynq.Server
	client *asynq.Client

	imageRepository *repositories.ImageRepository
	tagRepository   *repositories.TagRepository

	personService *services.PersonService
}

// Ensure Worker implements tasks.Client
var _ tasks.Client = (*Worker)(nil)

// NewWorker creates a new worker with the given container and repositories
func NewWorker(
	redisClient redis.UniversalClient,
	imageRepository *repositories.ImageRepository,
	tagRepository *repositories.TagRepository,
	personService *services.PersonService,
) (*Worker, error) {
	// Configure server with queues and priorities
	server := asynq.NewServerFromRedisClient(
		redisClient,
		asynq.Config{
			Queues: map[string]int{
				tasks.QueueReindex: 10,
			},
			Concurrency: 16,
			Logger:      nil,
		},
	)

	// Client for enqueuing tasks
	client := asynq.NewClientFromRedisClient(redisClient)

	return &Worker{
		server:          server,
		client:          client,
		imageRepository: imageRepository,
		tagRepository:   tagRepository,
		personService:   personService,
	}, nil
}

func (w *Worker) Start() error {
	mux := asynq.NewServeMux()

	mux.HandleFunc(string(tasks.TypeReindexImage), w.handleReindexImage)
	mux.HandleFunc(string(tasks.TypeReindexPerson), w.handleReindexPerson)
	mux.HandleFunc(string(tasks.TypeReindexTag), w.handleReindexTag)

	return w.server.Start(mux)
}

func (w *Worker) Stop() error {
	w.server.Shutdown()
	return w.client.Close()
}

func (w *Worker) enqueueReindex(ctx context.Context, taskType tasks.TaskType, uuid string) error {
	task := asynq.NewTask(string(taskType), []byte(uuid))

	_, err := w.client.EnqueueContext(
		ctx,
		task,
		asynq.MaxRetry(5),
		asynq.Timeout(3*time.Minute),
		asynq.Queue(tasks.QueueReindex),
		asynq.Retention(24*time.Hour),
		asynq.TaskID(fmt.Sprintf("%s:%s", string(taskType), uuid)),
	)

	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			log.Debug().Str("task", string(taskType)).Str("uuid", uuid).Msg("Reindex task already queued, skipping duplicate")
			return nil
		}
		return fmt.Errorf("error enqueueing task: %w", err)
	}

	log.Debug().Str("task", string(taskType)).Str("uuid", uuid).Msg("Successfully enqueued reindex task")

	return nil
}

func (w *Worker) EnqueueReindexImage(ctx context.Context, uuid string) error {
	if err := w.enqueueReindex(ctx, tasks.TypeReindexImage, uuid); err != nil {
		return fmt.Errorf("error enqueueing image reindex: %w", err)
	}

	return nil
}

func (w *Worker) EnqueueReindexPerson(ctx context.Context, uuid string) error {
	if err := w.enqueueReindex(ctx, tasks.TypeReindexPerson, uuid); err != nil {
		return fmt.Errorf("error enqueueing image reindex: %w", err)
	}

	return nil
}

func (w *Worker) EnqueueReindexTag(ctx context.Context, uuid string) error {
	if err := w.enqueueReindex(ctx, tasks.TypeReindexTag, uuid); err != nil {
		return fmt.Errorf("error enqueueing tag reindex: %w", err)
	}

	return nil
}

func (w *Worker) handleReindexImage(ctx context.Context, task *asynq.Task) error {
	uuid := string(task.Payload())

	log.Info().Str("uuid", uuid).Msg("Executing indexing job for image")

	image, err := w.imageRepository.GetByUUID(ctx, uuid)
	if err != nil {
		return fmt.Errorf("error getting image: %w", err)
	}

	if err := w.imageRepository.Reindex(ctx, image); err != nil {
		return fmt.Errorf("error reindexing image: %w", err)
	}

	return nil
}

func (w *Worker) handleReindexPerson(ctx context.Context, task *asynq.Task) error {
	uuid := string(task.Payload())

	log.Info().Str("uuid", uuid).Msg("Executing indexing job for person")

	person, err := w.personService.Get(ctx, uuid)
	if err != nil {
		return fmt.Errorf("error getting person: %w", err)
	}

	if err := w.personService.Index(ctx, person); err != nil {
		return fmt.Errorf("error reindexing person: %w", err)
	}

	return nil
}

func (w *Worker) handleReindexTag(ctx context.Context, task *asynq.Task) error {
	uuid := string(task.Payload())

	log.Info().Str("uuid", uuid).Msg("Executing indexing job for tag")

	tag, err := w.tagRepository.GetByUUID(ctx, uuid)
	if err != nil {
		return fmt.Errorf("error getting tag: %w", err)
	}

	if err := w.tagRepository.Reindex(ctx, tag); err != nil {
		return fmt.Errorf("error reindexing tag: %w", err)
	}

	return nil
}
