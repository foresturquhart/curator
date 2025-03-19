package elastic

import (
	"context"
	"embed"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/rs/zerolog/log"
)

//go:embed indexes/*
var indexFiles embed.FS

// Elastic represents an ElasticSearch client
type Elastic struct {
	client *elasticsearch.TypedClient
}

// NewElastic creates and configures a new ElasticSearch client
func NewElastic(connString string) (*Elastic, error) {
	// Configure OpenSearch client
	config := elasticsearch.Config{
		Addresses: []string{connString},
	}

	// Create the client
	client, err := elasticsearch.NewTypedClient(config)
	if err != nil {
		return nil, fmt.Errorf("unable to create Elasticsearch client: %w", err)
	}

	// Verify the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := client.Info().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Elasticsearch: %w", err)
	}

	log.Info().Msgf("Connected to Elasticsearch cluster %s", info.ClusterName)

	return &Elastic{client: client}, nil
}

// GetClient returns the underlying ElasticSearch client
func (elastic *Elastic) GetClient() *elasticsearch.TypedClient {
	return elastic.client
}

// EnsureIndex checks if an index exists, and creates it using the embedded JSON file if it does not.
func (elastic *Elastic) EnsureIndex(ctx context.Context, indexName string) error {
	exists, err := elastic.client.Indices.Exists(indexName).Do(ctx)
	if err != nil {
		return fmt.Errorf("error checking existence of index %s: %w", indexName, err)
	} else if exists {
		log.Info().Msgf("Index %s already exists", indexName)
		return nil
	}

	log.Info().Msgf("Index %s not found", indexName)

	// Read the mapping/settings from the embedded file.
	mapping, err := indexFiles.ReadFile(fmt.Sprintf("indexes/%s.json", indexName))
	if err != nil {
		return fmt.Errorf("unable to read file for index %s: %w", indexName, err)
	}

	// Create the index with the provided mapping.
	res, err := elastic.client.Indices.Create(indexName).Raw(strings.NewReader(string(mapping))).Do(ctx)
	if err != nil {
		return fmt.Errorf("failed to create index %s: %w", indexName, err)
	} else if !res.Acknowledged {
		return fmt.Errorf("failed to create index %s: not acknowledged", indexName)
	}

	log.Info().Msgf("Index %s created successfully", indexName)

	return nil
}
