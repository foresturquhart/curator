package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/services"
	"github.com/foresturquhart/curator/server/tasks"
	"github.com/hibiken/asynq"
	"github.com/rs/zerolog/log"
)

// Worker represents the background job processor
type Worker struct {
	server *asynq.Server
	client *asynq.Client

	imageRepository *repositories.ImageRepository

	personService *services.PersonService
	tagService    *services.TagService
}

// Ensure Worker implements tasks.Client
var _ tasks.Client = (*Worker)(nil)

// NewWorker creates a new worker with the given container and repositories
func NewWorker(
	container *container.Container,
	imageRepository *repositories.ImageRepository,
	personService *services.PersonService,
	tagService *services.TagService,
) (*Worker, error) {
	// Configure server with queues and priorities
	server := asynq.NewServerFromRedisClient(
		container.Redis.Client,
		asynq.Config{
			Queues: map[string]int{
				tasks.QueueReindex: 10,
			},
			Concurrency: 16,
			Logger:      nil,
		},
	)

	// Client for enqueuing tasks
	client := asynq.NewClientFromRedisClient(container.Redis.Client)

	return &Worker{
		server:          server,
		client:          client,
		imageRepository: imageRepository,
		personService:   personService,
		tagService:      tagService,
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

func (w *Worker) encodeIdPayload(id int64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, id)
	return buf.Bytes()
}

func (w *Worker) decodeIdPayload(b []byte) int64 {
	buf := bytes.NewReader(b)
	var n int64
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func (w *Worker) enqueueReindex(ctx context.Context, taskType tasks.TaskType, id int64) error {
	payload := w.encodeIdPayload(id)

	task := asynq.NewTask(string(taskType), []byte(payload))

	_, err := w.client.EnqueueContext(
		ctx,
		task,
		asynq.MaxRetry(5),
		asynq.Timeout(3*time.Minute),
		asynq.Queue(tasks.QueueReindex),
		asynq.Retention(24*time.Hour),
		asynq.TaskID(fmt.Sprintf("%s:%d", string(taskType), id)),
	)

	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			log.Debug().Str("task", string(taskType)).Int64("id", id).Msg("Reindex task already queued, skipping duplicate")
			return nil
		}
		return fmt.Errorf("error enqueueing task: %w", err)
	}

	log.Debug().Str("task", string(taskType)).Int64("id", id).Msg("Successfully enqueued reindex task")

	return nil
}

func (w *Worker) EnqueueReindexImage(ctx context.Context, id int64) error {
	if err := w.enqueueReindex(ctx, tasks.TypeReindexImage, id); err != nil {
		return fmt.Errorf("error enqueueing image reindex: %w", err)
	}

	return nil
}

func (w *Worker) EnqueueReindexPerson(ctx context.Context, id int64) error {
	if err := w.enqueueReindex(ctx, tasks.TypeReindexPerson, id); err != nil {
		return fmt.Errorf("error enqueueing image reindex: %w", err)
	}

	return nil
}

func (w *Worker) EnqueueReindexTag(ctx context.Context, id int64) error {
	if err := w.enqueueReindex(ctx, tasks.TypeReindexTag, id); err != nil {
		return fmt.Errorf("error enqueueing tag reindex: %w", err)
	}

	return nil
}

func (w *Worker) handleReindexImage(ctx context.Context, task *asynq.Task) error {
	id := w.decodeIdPayload(task.Payload())

	log.Info().Int64("id", id).Msg("Executing indexing job for image")

	image, err := w.imageRepository.GetByID(ctx, id)
	if err != nil {
		return fmt.Errorf("error getting image: %w", err)
	}

	if err := w.imageRepository.Index(ctx, image); err != nil {
		return fmt.Errorf("error reindexing image: %w", err)
	}

	return nil
}

func (w *Worker) handleReindexPerson(ctx context.Context, task *asynq.Task) error {
	id := w.decodeIdPayload(task.Payload())

	log.Info().Int64("id", id).Msg("Executing indexing job for person")

	person, err := w.personService.GetByInternalID(ctx, id)
	if err != nil {
		return fmt.Errorf("error getting person: %w", err)
	}

	if err := w.personService.Index(ctx, person); err != nil {
		return fmt.Errorf("error reindexing person: %w", err)
	}

	return nil
}

func (w *Worker) handleReindexTag(ctx context.Context, task *asynq.Task) error {
	id := w.decodeIdPayload(task.Payload())

	log.Info().Int64("id", id).Msg("Executing indexing job for tag")

	tag, err := w.tagService.GetByInternalID(ctx, id)
	if err != nil {
		return fmt.Errorf("error getting person: %w", err)
	}

	if err := w.tagService.Index(ctx, tag); err != nil {
		return fmt.Errorf("error reindexing tag: %w", err)
	}

	return nil
}
