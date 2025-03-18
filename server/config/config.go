package config

import (
	"github.com/caarlos0/env/v6"
)

type Config struct {
	Port     int    `env:"PORT" envDefault:"8080"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	EncryptionKey string `env:"ENCRYPTION_KEY" envDefault:"secret"`

	DatabaseURL    string `env:"DATABASE_URL" envDefault:"postgresql://postgres:postgres@127.0.0.1:5432/postgres"`
	OpenSearchURL  string `env:"OPENSEARCH_URL" envDefault:"http://127.0.0.1:9200"`
	ClipServiceURL string `env:"CLIP_SERVICE_URL" envDefault:"127.0.0.1:6200"`

	QdrantHost string `env:"QDRANT_HOST" envDefault:"127.0.0.1"`
	QdrantPort int    `env:"QDRANT_PORT" envDefault:"6334"`

	FileStoragePath string `env:"FILE_STORAGE_PATH" envDefault:"./images"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
