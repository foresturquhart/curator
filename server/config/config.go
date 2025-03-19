package config

import (
	"github.com/caarlos0/env/v6"
)

type Config struct {
	Port     int    `env:"PORT" envDefault:"8080"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	EncryptionKey string `env:"ENCRYPTION_KEY" envDefault:"secret"`

	DatabaseURL      string `env:"DATABASE_URL" envDefault:"postgresql://postgres:postgres@127.0.0.1:5432/postgres"`
	ElasticsearchURL string `env:"ELASTICSEARCH_URL" envDefault:"http://127.0.0.1:9200"`

	QdrantHost string `env:"QDRANT_HOST" envDefault:"127.0.0.1"`
	QdrantPort int    `env:"QDRANT_PORT" envDefault:"6334"`

	ClipHost string `env:"CLIP_HOST" envDefault:"127.0.0.1"`
	ClipPort int    `env:"CLIP_PORT" envDefault:"50051"`

	StoragePath string `env:"STORAGE_PATH" envDefault:"./images"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
