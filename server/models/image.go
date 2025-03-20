package models

import (
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/pgvector/pgvector-go"
)

// ImageFormat represents valid image file formats
type ImageFormat string

// Valid image formats
const (
	FormatJPEG ImageFormat = "jpeg"
	FormatPNG  ImageFormat = "png"
	FormatGIF  ImageFormat = "gif"
)

// SortDirection specifies the sort order
type SortDirection string

// Sort direction constants
const (
	SortDirectionAsc  SortDirection = "asc"
	SortDirectionDesc SortDirection = "desc"
)

// SortBy specifies the field to sort by
type SortBy string

// Sort field constants
const (
	SortByRelevance  SortBy = "_score"
	SortByCreatedAt  SortBy = "created_at"
	SortByTitle      SortBy = "title.keyword"
	SortByTagCount   SortBy = "tag_count"
	SortByDimensions SortBy = "pixel_count"
	SortByRandom     SortBy = "random"
)

// PaginatedImageResult represents a paginated result set
type PaginatedImageResult struct {
	Data       []*Image           `json:"data"`        // The actual result images
	HasMore    bool               `json:"has_more"`    // Whether there are more results available
	TotalCount int64              `json:"total_count"` // Total count of matching images
	NextCursor []types.FieldValue `json:"next_cursor"` // Cursor for fetching the next page
}

// Image represents an image entity in the system
type Image struct {
	ID          int64            `json:"-"`           // Internal primary key
	UUID        string           `json:"id"`          // Public-facing identifier
	Filename    string           `json:"filename"`    // Original filename
	MD5         string           `json:"md5"`         // MD5 hash
	SHA1        string           `json:"sha1"`        // SHA1 hash
	Width       int              `json:"width"`       // Width in pixels
	Height      int              `json:"height"`      // Height in pixels
	Format      ImageFormat      `json:"format"`      // File format
	Size        int64            `json:"size"`        // File size in bytes
	Embedding   *pgvector.Vector `json:"-"`           // Vector embedding (512 dimensions)
	Title       *string          `json:"title"`       // Optional user-provided title
	Description *string          `json:"description"` // Optional user-provided description
	CreatedAt   time.Time        `json:"created_at"`  // Creation timestamp
	UpdatedAt   time.Time        `json:"updated_at"`  // Last update timestamp

	Tags    []*ImageTag    `json:"tags"`    // Associated tags
	People  []*ImagePerson `json:"people"`  // Associated people with roles
	Sources []*ImageSource `json:"sources"` // Associated sources
}

func (i *Image) GetStoredName() string {
	// Determine file path and extension
	var ext string
	switch i.Format {
	case FormatJPEG:
		ext = ".jpg"
	case FormatPNG:
		ext = ".png"
	case FormatGIF:
		ext = ".gif"
	}

	return i.UUID + ext
}

// GetID gets the ID of the image
func (i *Image) GetID() int64 {
	return i.ID
}

// GetUUID gets the UUID of the image
func (i *Image) GetUUID() string {
	return i.UUID
}

// ImageTag represents a tag associated with an image
type ImageTag struct {
	ID          int64     `json:"-"`           // Internal primary key
	UUID        string    `json:"id"`          // Public-facing identifier
	Name        string    `json:"name"`        // Tag name
	Description *string   `json:"description"` // Tag description
	AddedAt     time.Time `json:"added_at"`    // Addition timestamp
}

// PersonRole represents the role a person has in relation to an image
type PersonRole string

// Valid person roles
const (
	RoleCreator PersonRole = "creator"
	RoleSubject PersonRole = "subject"
)

// ImagePerson represents a person associated with an image in a specific role
type ImagePerson struct {
	ID          int64      `json:"-"`           // Internal primary key
	UUID        string     `json:"id"`          // Public-facing identifier
	Name        string     `json:"name"`        // Person name
	Description *string    `json:"description"` // Person description
	Role        PersonRole `json:"role"`        // Their role (creator or subject)
	AddedAt     time.Time  `json:"added_at"`    // Addition timestamp
}

// ImageSource represents a source associated with an image
type ImageSource struct {
	URL         string  `json:"url"`         // Source URL
	Title       *string `json:"title"`       // Optional source title
	Description *string `json:"description"` // Optional source description
}

// ImageTagFilter represents a filter condition for a tag
type ImageTagFilter struct {
	ID      string `json:"id"`      // Tag name or UUID
	Include bool   `json:"include"` // Whether to include (true) or exclude (false)
}

// ImagePersonFilter represents a filter condition for a tag
type ImagePersonFilter struct {
	ID      string      `json:"id"`      // Person UUID
	Include bool        `json:"include"` // Whether to include (true) or exclude (false)
	Role    *PersonRole `json:"role"`    // Filter by role (creator or subject, optional)
}

// ImageFilter represents the filtering options for image queries
type ImageFilter struct {
	// Filtering fields
	Title              string              // Search by title
	Description        string              // Search by description
	Source             string              // Search by source
	Hash               string              // Search by MD5 or SHA1 hash
	MinWidth           int                 // Minimum width in pixels
	MaxWidth           int                 // Maximum width in pixels
	MinHeight          int                 // Minimum height in pixels
	MaxHeight          int                 // Maximum height in pixels
	SinceDate          *time.Time          // Filter for images created after this date
	BeforeDate         *time.Time          // Filter for images created before this date
	SimilarToID        string              // Find images similar to the image with this UUID
	SimilarToEmbedding *pgvector.Vector    // Find images similar to this embedding vector
	TagFilters         []ImageTagFilter    // Tags to include or exclude
	PersonFilters      []ImagePersonFilter // People to include or exclude

	// Similarity threshold field
	SimilarityThreshold float64

	// Sorting fields
	SortBy        SortBy        // Field to sort by (default: created_at)
	SortDirection SortDirection // Sort direction (default: desc)

	// Random sorting seed field
	RandomSeed *string

	// Pagination fields
	Limit         int                // Maximum number of results (default: 50, max: 100)
	StartingAfter []types.FieldValue // Cursor to start after (forward pagination)
}
