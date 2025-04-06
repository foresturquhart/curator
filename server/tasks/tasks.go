package tasks

import "context"

// Task types
type TaskType string

const (
	TypeReindexImage  TaskType = "reindex:image"
	TypeReindexPerson TaskType = "reindex:person"
	TypeReindexTag    TaskType = "reindex:tag"
)

// Queue name
const QueueReindex = "reindex"

// Client defines an interface for enqueuing tasks
type Client interface {
	// EnqueueReindexImage adds a job to reindex a single image
	EnqueueReindexImage(ctx context.Context, id int64) error

	// EnqueueReindexPerson adds a job to reindex a person
	EnqueueReindexPerson(ctx context.Context, id int64) error

	// EnqueueReindexTag adds a job to reindex a tag
	EnqueueReindexTag(ctx context.Context, id int64) error
}
