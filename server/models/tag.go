package models

import "time"

// Tag represents a tag entity in the system
type Tag struct {
	ID            int64     `json:"-"`                        // Internal primary key
	UUID          string    `json:"id"`                       // Public-facing identifier
	Name          string    `json:"name"`                     // Tag name
	Description   string    `json:"description"`              // Tag description
	CanonicalID   *int64    `json:"-"`                        // Reference to canonical form of this tag (if any)
	Canonical     *Tag      `json:"canonical,omitempty"`      // Canonical form of the tag (if any)
	RootCanonical *Tag      `json:"root_canonical,omitempty"` // Ultimate canonical form (if different from immediate canonical)
	CreatedAt     time.Time `json:"created_at"`               // Creation timestamp
	UpdatedAt     time.Time `json:"updated_at"`               // Last update timestamp
}

// TagFilter represents the filtering options for tag queries
type TagFilter struct {
	Name        string     // Search by name
	Description string     // Search by description
	SinceDate   *time.Time // Filter for tags created after this date
	BeforeDate  *time.Time // Filter for tags created before this date
	// IDEA: filter by direct ancestor or by distant ancestor
}
