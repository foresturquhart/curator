package models

import "time"

// Person represents a person entity in the system
type Person struct {
	ID          int64     `json:"-"`           // Internal primary key
	UUID        string    `json:"id"`          // Public-facing identifier
	Name        string    `json:"name"`        // Person name
	Description *string   `json:"description"` // Person description
	CreatedAt   time.Time `json:"created_at"`  // Creation timestamp
	UpdatedAt   time.Time `json:"updated_at"`  // Last update timestamp

	Sources []*PersonSource `json:"sources"` // Associated sources
}

// PersonSource represents a source associated with a person
type PersonSource struct {
	URL         string  `json:"url"`         // Source URL
	Title       *string `json:"title"`       // Optional source title
	Description *string `json:"description"` // Optional source description
}

// PersonFilter represents the filtering options for person queries
type PersonFilter struct{}
