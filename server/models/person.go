package models

import (
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

// Person represents a person entity in the system
type Person struct {
	ID          int64     `json:"-"`           // Internal primary key
	UUID        string    `json:"id"`          // Public-facing identifier
	Name        string    `json:"name"`        // Person name
	Description *string   `json:"description"` // Person description
	CreatedAt   time.Time `json:"created_at"`  // Creation timestamp
	UpdatedAt   time.Time `json:"updated_at"`  // Last update timestamp

	Sources []*PersonSource `json:"sources"` // Associated sources

	// Computed fields
	CreatorImageCount int `json:"creator_image_count,omitempty"` // Count of images where person is creator
	SubjectImageCount int `json:"subject_image_count,omitempty"` // Count of images where person is subject
}

// PersonSource represents a source associated with a person
type PersonSource struct {
	URL         string  `json:"url"`         // Source URL
	Title       *string `json:"title"`       // Optional source title
	Description *string `json:"description"` // Optional source description
}

// SortBy specifies the field to sort by
type PersonSortBy string

// Sort field constants for people
const (
	PersonSortByRelevance    PersonSortBy = "_score"
	PersonSortByCreatedAt    PersonSortBy = "created_at"
	PersonSortByName         PersonSortBy = "name.keyword"
	PersonSortByCreatorCount PersonSortBy = "creator_count"
	PersonSortBySubjectCount PersonSortBy = "subject_count"
)

// PaginatedPersonResult represents a paginated result set
type PaginatedPersonResult struct {
	Data       []*Person          `json:"data"`        // The actual result people
	HasMore    bool               `json:"has_more"`    // Whether there are more results available
	TotalCount int64              `json:"total_count"` // Total count of matching people
	NextCursor []types.FieldValue `json:"next_cursor"` // Cursor for fetching the next page
}

// PersonFilter represents the filtering options for person queries
type PersonFilter struct {
	// Filtering fields
	Name        string     // Search by name
	Description string     // Search by description
	Source      string     // Search by source URL
	SinceDate   *time.Time // Filter for people created after this date
	BeforeDate  *time.Time // Filter for people created before this date

	// Sorting fields
	SortBy        PersonSortBy  // Field to sort by (default: created_at)
	SortDirection SortDirection // Sort direction (default: desc)

	// Pagination fields
	Limit         int                // Maximum number of results (default: 50, max: 100)
	StartingAfter []types.FieldValue // Cursor to start after (forward pagination)
}
