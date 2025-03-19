package container

import (
	"fmt"

	"github.com/foresturquhart/curator/server/clip"
	"github.com/foresturquhart/curator/server/config"
	"github.com/foresturquhart/curator/server/storage"
	"github.com/foresturquhart/curator/server/storage/elastic"
)

type Container struct {
	Config   *config.Config
	Database *storage.Database
	Elastic  *elastic.Elastic
	Qdrant   *storage.Qdrant
	Clip     *clip.Client
}

func NewContainer(cfg *config.Config) (*Container, error) {
	// Initialize database client
	databaseClient, err := storage.NewDatabase(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize elastic client
	elasticClient, err := elastic.NewElastic(cfg.ElasticsearchURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize elastic: %w", err)
	}

	// Initialize qdrant client
	qdrantClient, err := storage.NewQdrant(cfg.QdrantHost, cfg.QdrantPort)
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
