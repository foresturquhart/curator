package clip

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn       *grpc.ClientConn
	clipClient CLIPServiceClient
}

func NewClient(addr string) (*Client, error) {
	// Connect to the gRPC server.
	clientConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	// Create the gRPC client stub.
	client := NewCLIPServiceClient(clientConn)
	return &Client{
		conn:       clientConn,
		clipClient: client,
	}, nil
}

// GetEmbeddingFromImageData sends image data to the CLIP service and returns the embedding
func (c *Client) GetEmbeddingFromImageData(ctx context.Context, imageData []byte) ([]float32, error) {
	if len(imageData) == 0 {
		return nil, fmt.Errorf("empty image data")
	}

	req := &ImageRequest{
		ImageData: imageData,
	}

	resp, err := c.clipClient.GetImageEmbedding(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get image embedding: %w", err)
	}

	return resp.Embedding, nil
}

// GetEmbeddingFromReader reads from a reader (like a file upload) and gets the embedding
func (c *Client) GetEmbeddingFromReader(ctx context.Context, reader io.Reader) ([]float32, error) {
	imageData, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read image data: %w", err)
	}

	return c.GetEmbeddingFromImageData(ctx, imageData)
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
