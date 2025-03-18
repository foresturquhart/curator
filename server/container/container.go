package container

import (
	"fmt"

	"github.com/foresturquhart/curator/server/clip"
	"github.com/foresturquhart/curator/server/config"
	"github.com/foresturquhart/curator/server/storage"
)

type Container struct {
	Config     *config.Config
	Database   *storage.Database
	OpenSearch *storage.OpenSearch
	Qdrant     *storage.Qdrant
	Clip       *clip.Client
}

func NewContainer(cfg *config.Config) (*Container, error) {
	// Initialize database client
	databaseClient, err := storage.NewDatabase(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize opensearch client
	openSearchClient, err := storage.NewOpenSearch(cfg.OpenSearchURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize opensearch: %w", err)
	}

	// Initialize qdrant client
	qdrantClient, err := storage.NewQdrant(cfg.QdrantHost, cfg.QdrantPort)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize qdrant: %w", err)
	}

	// Initialize clip service client
	clipServiceClient, err := clip.NewClient(cfg.ClipServiceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize clip service: %w", err)
	}

	return &Container{
		Config:     cfg,
		Database:   databaseClient,
		OpenSearch: openSearchClient,
		Qdrant:     qdrantClient,
		Clip:       clipServiceClient,
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
