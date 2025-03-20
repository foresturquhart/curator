package elastic

import (
	"context"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/foresturquhart/curator/server/elastic/indexes"
)

type Elastic struct {
	Client *elasticsearch.TypedClient
}

func NewElastic(cfg elasticsearch.Config) (*Elastic, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create elasticsearch client: %w", err)
	}

	if _, err := client.Info().Do(ctx); err != nil {
		return nil, fmt.Errorf("unable to connect to elasticsearch: %w", err)
	}

	return &Elastic{
		Client: client,
	}, nil
}

func (e *Elastic) Migrate(ctx context.Context) error {
	for name, mapping := range indexes.Indexes {
		exists, err := e.Client.Indices.Exists(name).Do(ctx)
		if err != nil {
			return fmt.Errorf("unable to check if index %s exists: %w", name, err)
		}

		if !exists {
			res, err := e.Client.Indices.Create(name).Mappings(mapping).Do(ctx)
			if err != nil {
				return fmt.Errorf("failed to create index %s: %w", name, err)
			} else if !res.Acknowledged {
				return fmt.Errorf("failed to create index %s: not acknowledged", name)
			}
		} else {
			res, err := e.Client.Indices.PutMapping(name).Properties(mapping.Properties).Do(ctx)
			if err != nil {
				return fmt.Errorf("failed to update index %s: %w", name, err)
			} else if !res.Acknowledged {
				return fmt.Errorf("failed to update index %s: not acknowledged", name)
			}
		}
	}

	return nil
}
