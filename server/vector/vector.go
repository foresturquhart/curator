package vector

import (
	"context"
	"fmt"

	"github.com/qdrant/go-client/qdrant"
)

type Qdrant struct {
	Client *qdrant.Client
}

func NewQdrant(cfg *qdrant.Config) (*Qdrant, error) {
	client, err := qdrant.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create Qdrant client: %w", err)
	}

	return &Qdrant{
		Client: client,
	}, nil
}

func (q *Qdrant) Close() error {
	return q.Client.Close()
}

func (q *Qdrant) Migrate(ctx context.Context) error {
	exists, err := q.Client.CollectionExists(ctx, "images")
	if err != nil {
		return fmt.Errorf("unable to check if index images exists: %w", err)
	}

	if !exists {
		err := q.Client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: "images",
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     512,
				Distance: qdrant.Distance_Cosine,
			}),
		})

		if err != nil {
			return fmt.Errorf("ailed to create index images: %w", err)
		}
	}

	return nil
}
