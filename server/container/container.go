package container

import (
	"context"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/foresturquhart/curator/server/clip"
	"github.com/foresturquhart/curator/server/config"
	"github.com/foresturquhart/curator/server/database"
	"github.com/foresturquhart/curator/server/elastic"
	"github.com/foresturquhart/curator/server/vector"
	"github.com/qdrant/go-client/qdrant"
)

type Container struct {
	Config   *config.Config
	Database *database.Database
	Elastic  *elastic.Elastic
	Qdrant   *vector.Qdrant
	Clip     *clip.Client
}

func NewContainer(cfg *config.Config) (*Container, error) {
	// Initialize database client
	databaseClient, err := database.NewDatabase(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize elastic client
	elasticClient, err := elastic.NewElastic(elasticsearch.Config{
		Addresses: []string{cfg.ElasticsearchURL},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize elasticsearch: %w", err)
	}

	// Initialize qdrant client
	qdrantClient, err := vector.NewQdrant(&qdrant.Config{
		Host: cfg.QdrantHost,
		Port: cfg.QdrantPort,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize qdrant: %w", err)
	}

	// Initialize clip client
	clipClient, err := clip.NewClient(fmt.Sprintf("%s:%d", cfg.ClipHost, cfg.ClipPort))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize clip: %w", err)
	}

	return &Container{
		Config:   cfg,
		Database: databaseClient,
		Elastic:  elasticClient,
		Qdrant:   qdrantClient,
		Clip:     clipClient,
	}, nil
}

// Close gracefully shuts down all container resources
func (c *Container) Close() {
	if c.Clip != nil {
		c.Clip.Close()
	}

	if c.Qdrant != nil {
		c.Qdrant.Close()
	}

	if c.Database != nil {
		c.Database.Close()
	}
}

func (c *Container) Migrate(ctx context.Context) error {
	if err := c.Database.Migrate(); err != nil {
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
