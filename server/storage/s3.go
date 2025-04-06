package storage

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Config struct {
	Endpoint        string
	AccessKeyID     string
	Region          string
	SecretAccessKey string
	UseSSL          bool
	ForcePathStyle  bool
	Bucket          string
	CreateBucket    bool
}

type S3 struct {
	client *minio.Client
	config *S3Config
}

func NewS3(ctx context.Context, config *S3Config) (*S3, error) {
	parsedEndpoint, err := url.Parse(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint url: %w", err)
	}

	if parsedEndpoint.Scheme == "" {
		if config.UseSSL {
			parsedEndpoint.Scheme = "https"
		} else {
			parsedEndpoint.Scheme = "http"
		}
		config.Endpoint = parsedEndpoint.Scheme + "://" + parsedEndpoint.Host
	} else {
		config.Endpoint = parsedEndpoint.Scheme + "://" + parsedEndpoint.Host
	}

	client, err := minio.New(parsedEndpoint.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
		Region: config.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	if config.CreateBucket {
		exists, err := client.BucketExists(ctx, config.Bucket)
		if err != nil {
			return nil, fmt.Errorf("failed to check bucket existence: %w", err)
		}

		if !exists {
			err = client.MakeBucket(ctx, config.Bucket, minio.MakeBucketOptions{
				Region: config.Region,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create bucket '%s': %w", config.Bucket, err)
			}
		}
	}

	return &S3{
		client: client,
		config: config,
	}, nil
}

func (s *S3) Upload(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, s.config.Bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to upload object '%s' to bucket '%s': %w", key, s.config.Bucket, err)
	}
	return nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.config.Bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete object '%s' from bucket '%s': %w", key, s.config.Bucket, err)
	}
	return nil
}

func (s *S3) GetPublicURL(key string) (string, error) {
	parsedEndpoint, err := url.Parse(s.config.Endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL: %w", err)
	}

	if s.config.ForcePathStyle {
		parsedEndpoint.Path = fmt.Sprintf("/%s/%s", s.config.Bucket, key)
	} else {
		parsedEndpoint.Host = fmt.Sprintf("%s.%s", s.config.Bucket, parsedEndpoint.Host)
		parsedEndpoint.Path = "/" + key
	}
	return parsedEndpoint.String(), nil
}
