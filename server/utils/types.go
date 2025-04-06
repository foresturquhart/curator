package utils

import "github.com/elastic/go-elasticsearch/v8/typedapi/types"

// SortDirection specifies the sort order
type SortDirection string

// Sort direction constants
const (
	SortDirectionAsc  SortDirection = "asc"
	SortDirectionDesc SortDirection = "desc"
)

type PaginationOptions struct {
	Limit         int
	StartingAfter []types.FieldValue
}

type PaginatedResult[T any] struct {
	Data       []T                `json:"data"`
	HasMore    bool               `json:"has_more"`
	TotalCount int64              `json:"total_count"`
	NextCursor []types.FieldValue `json:"next_cursor,omitempty"`
}
