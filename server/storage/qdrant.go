package storage

import (
	"fmt"

	"github.com/qdrant/go-client/qdrant"
)

// Qdrant represents an Qdrant client
type Qdrant struct {
	client *qdrant.Client
}

// NewQdrant creates and configures a new Qdrant client
func NewQdrant(host string, port int) (*Qdrant, error) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})

	if err != nil {
		return nil, fmt.Errorf("unable to create Qdrant client: %w", err)
	}

	return &Qdrant{client: client}, nil
}

// GetClient returns the underlying Qdrant PointsClient
func (q *Qdrant) GetClient() *qdrant.Client {
	return q.client
}

// Close closes the Qdrant gRPC connection
func (q *Qdrant) Close() error {
	return q.client.Close()
}
