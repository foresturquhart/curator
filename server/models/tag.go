package models

import "time"

// Tag represents a tag entity in the system
type Tag struct {
	ID          int64     `json:"id"`          // Internal primary key
	UUID        string    `json:"uuid"`        // Public-facing identifier
	Name        string    `json:"name"`        // Tag name
	Description string    `json:"description"` // Tag description
	ParentID    *int64    `json:"-"`
	Position    int64     `json:"-"`
	CreatedAt   time.Time `json:"created_at"` // Creation timestamp
	UpdatedAt   time.Time `json:"updated_at"` // Last update timestamp
}

// TagNode represents a tag in the tree with its nested children.
// This is an output type that extends models.Tag with tree information.
type TagNode struct {
	Tag
	Children []*TagNode `json:"children,omitempty"`
}

// TagFilter represents the filtering options for tag queries
type TagFilter struct {
	Name        string     // Full text search by name
	Description string     // Full text search by description
	SinceDate   *time.Time // Filter for tags created after this date
	BeforeDate  *time.Time // Filter for tags created before this date
}
