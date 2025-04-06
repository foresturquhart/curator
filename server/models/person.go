package models

import (
	"time"
)

// Person represents a person entity in the system
type Person struct {
	ID          int64     `json:"id"`
	UUID        string    `json:"uuid"`
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`

	// Nested fields
	Sources []*PersonSource `json:"sources"`
}

type PersonSource struct {
	URL         string  `json:"url"`
	Title       *string `json:"title"`
	Description *string `json:"description"`
}

// ToSearchRecord converts a Person domain model to a PersonSearchRecord.
func (p *Person) ToSearchRecord() *PersonSearchRecord {
	record := &PersonSearchRecord{
		ID:          p.ID,
		UUID:        p.UUID,
		Name:        p.Name,
		Description: p.Description,
		CreatedAt:   p.CreatedAt,
		UpdatedAt:   p.UpdatedAt,
	}
	for _, src := range p.Sources {
		record.Sources = append(record.Sources, &PersonSearchRecordSource{
			URL:         src.URL,
			Title:       src.Title,
			Description: src.Description,
		})
	}
	return record
}

// PersonSearchRecord represents the document format used by Elasticsearch.
type PersonSearchRecord struct {
	ID          int64                       `json:"id"`
	UUID        string                      `json:"uuid"`
	Name        string                      `json:"name"`
	Description *string                     `json:"description"`
	CreatedAt   time.Time                   `json:"created_at"`
	UpdatedAt   time.Time                   `json:"updated_at"`
	Sources     []*PersonSearchRecordSource `json:"sources"`
}

// PersonSearchRecordSource mirrors nested source objects.
type PersonSearchRecordSource struct {
	URL         string  `json:"url"`
	Title       *string `json:"title"`
	Description *string `json:"description"`
}

func (r *PersonSearchRecord) ToModel() *Person {
	person := &Person{
		ID:          r.ID,
		UUID:        r.UUID,
		Name:        r.Name,
		Description: r.Description,
		CreatedAt:   r.CreatedAt,
		UpdatedAt:   r.UpdatedAt,
		Sources:     make([]*PersonSource, len(r.Sources)),
	}
	for i, src := range r.Sources {
		person.Sources[i] = &PersonSource{
			URL:         src.URL,
			Title:       src.Title,
			Description: src.Description,
		}
	}
	return person
}
