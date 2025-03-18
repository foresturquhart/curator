package storage

import (
	"context"
	"fmt"
	"github.com/opensearch-project/opensearch-go/v2"
	"time"
)

// OpenSearch represents an OpenSearch client
type OpenSearch struct {
	client *opensearch.Client
}

// NewOpenSearch creates and configures a new OpenSearch client
func NewOpenSearch(url string) (*OpenSearch, error) {
	// Configure OpenSearch client
	config := opensearch.Config{
		Addresses: []string{url},
	}

	// Create the client
	client, err := opensearch.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("unable to create OpenSearch client: %w", err)
	}

	// Verify the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Info(client.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to OpenSearch: %w", err)
	}

	return &OpenSearch{client: client}, nil
}

// GetClient returns the underlying OpenSearch client
func (os *OpenSearch) GetClient() *opensearch.Client {
	return os.client
}
