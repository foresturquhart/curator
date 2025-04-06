package config

import (
	"github.com/caarlos0/env/v6"
)

type Config struct {
	Port     int    `env:"PORT" envDefault:"8080"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	EncryptionKey string `env:"ENCRYPTION_KEY" envDefault:"secret"`

	PostgresURL string `env:"POSTGRES_URL" envDefault:"postgresql://curator:curator@127.0.0.1:5432/curator"`

	ElasticsearchURL string `env:"ELASTICSEARCH_URL" envDefault:"http://127.0.0.1:9200"`

	QdrantHost string `env:"QDRANT_HOST" envDefault:"127.0.0.1"`
	QdrantPort int    `env:"QDRANT_PORT" envDefault:"6334"`

	RedisAddr     string `env:"REDIS_ADDR" envDefault:"127.0.0.1:6379"`
	RedisPassword string `env:"REDIS_PASSWORD"`
	RedisDatabase int    `env:"REDIS_DATABASE" envDefault:"0"`

	ClipHost string `env:"CLIP_HOST" envDefault:"127.0.0.1"`
	ClipPort int    `env:"CLIP_PORT" envDefault:"50051"`

	S3Endpoint        string `env:"S3_ENDPOINT" envDefault:"http://127.0.0.1:9000"`
	S3AccessKeyID     string `env:"S3_ACCESS_KEY_ID" envDefault:"minioadmin"`
	S3Region          string `env:"S3_REGION" envDefault:"eu-west-1"`
	S3SecretAccessKey string `env:"S3_SECRET_ACCESS_KEY" envDefault:"minioadmin"`
	S3UseSSL          bool   `env:"S3_USE_SSL" envDefault:"false"`
	S3ForcePathStyle  bool   `env:"S3_FORCE_PATH_STYLE" envDefault:"true"`
	S3Bucket          string `env:"S3_BUCKET" envDefault:"curator"`
	S3CreateBucket    bool   `env:"S3_CREATE_BUCKET" envDefault:"true"`
}

func Load() (*Config, error) {
	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
