package container

import (
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/foresturquhart/curator/server/clip"
	"github.com/foresturquhart/curator/server/config"
	"github.com/foresturquhart/curator/server/storage"
	"github.com/foresturquhart/curator/server/tasks"
	"github.com/qdrant/go-client/qdrant"
	"github.com/redis/go-redis/v9"
)

type Container struct {
	Config   *config.Config
	Postgres *storage.Postgres
	Elastic  *storage.Elastic
	Qdrant   *storage.Qdrant
	Redis    *storage.Redis
	S3       *storage.S3
	Clip     *clip.Client
	Worker   tasks.Client
}

func NewContainer(ctx context.Context, cfg *config.Config) (*Container, error) {
	// Initialize postgres client
	postgresClient, err := storage.NewPostgres(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres: %w", err)
	}

	// Initialize elastic client
	elasticClient, err := storage.NewElastic(elasticsearch.Config{
		Addresses: []string{cfg.ElasticsearchURL},
		// Logger:    &CustomLogger{log},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize elasticsearch: %w", err)
	}

	// Initialize qdrant client
	qdrantClient, err := storage.NewQdrant(&qdrant.Config{
		Host: cfg.QdrantHost,
		Port: cfg.QdrantPort,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize qdrant: %w", err)
	}

	// Initialize redis client
	redisClient, err := storage.NewRedis(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDatabase,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize redis: %w", err)
	}

	// Initialize s3 client
	s3Client, err := storage.NewS3(ctx, &storage.S3Config{
		Endpoint:        cfg.S3Endpoint,
		AccessKeyID:     cfg.S3AccessKeyID,
		Region:          cfg.S3Region,
		SecretAccessKey: cfg.S3SecretAccessKey,
		UseSSL:          cfg.S3UseSSL,
		ForcePathStyle:  cfg.S3ForcePathStyle,
		Bucket:          cfg.S3Bucket,
		CreateBucket:    cfg.S3CreateBucket,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize s3: %w", err)
	}

	// Initialize clip client
	clipClient, err := clip.NewClient(fmt.Sprintf("%s:%d", cfg.ClipHost, cfg.ClipPort))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize clip: %w", err)
	}

	return &Container{
		Config:   cfg,
		Postgres: postgresClient,
		Elastic:  elasticClient,
		Qdrant:   qdrantClient,
		Redis:    redisClient,
		S3:       s3Client,
		Clip:     clipClient,
	}, nil
}

// Close gracefully shuts down all container resources
func (c *Container) Close() {
	if c.Clip != nil {
		c.Clip.Close()
	}

	if c.Redis != nil {
		c.Redis.Close()
	}

	if c.Qdrant != nil {
		c.Qdrant.Close()
	}

	if c.Postgres != nil {
		c.Postgres.Close()
	}
}

func (c *Container) Migrate(ctx context.Context) error {
	if err := c.Postgres.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate database: %w", err)
	}

	if err := c.Elastic.Migrate(ctx); err != nil {
		return fmt.Errorf("failed to migrate elasticsearch: %w", err)
	}

	if err := c.Qdrant.Migrate(ctx); err != nil {
		return fmt.Errorf("failed to migrate qdrant: %w", err)
	}

	return nil
}
