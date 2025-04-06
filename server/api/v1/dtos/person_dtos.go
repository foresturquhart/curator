package dtos

import (
	"time"

	"github.com/foresturquhart/curator/server/models"
	"github.com/go-playground/validator/v10"
)

var Validate = validator.New()

type PersonCreateRequest struct {
	Name        string                `json:"name" validate:"required,min=1"`
	Description *string               `json:"description,omitempty"`
	Sources     []PersonSourceRequest `json:"sources,omitempty" validate:"dive"`
}

func (r *PersonCreateRequest) ToModel() *models.Person {
	sources := make([]*models.PersonSource, len(r.Sources))
	for i, src := range r.Sources {
		sources[i] = &models.PersonSource{
			URL:         src.URL,
			Title:       src.Title,
			Description: src.Description,
		}
	}
	return &models.Person{
		Name:        r.Name,
		Description: r.Description,
		Sources:     sources,
	}
}

type PersonUpdateRequest struct {
	Name        *string               `json:"name,omitempty" validate:"omitempty,min=1"`
	Description *string               `json:"description,omitempty"`
	Sources     []PersonSourceRequest `json:"sources,omitempty" validate:"dive"`
}

func (r *PersonUpdateRequest) UpdateModel(person *models.Person) {
	if r.Name != nil {
		person.Name = *r.Name
	}
	if r.Description != nil {
		person.Description = r.Description
	}
	if r.Sources != nil {
		sources := make([]*models.PersonSource, len(r.Sources))
		for i, src := range r.Sources {
			sources[i] = &models.PersonSource{
				URL:         src.URL,
				Title:       src.Title,
				Description: src.Description,
			}
		}
		person.Sources = sources
	}
}

type PersonListRequest struct {
	Limit         *int    `query:"limit"`
	StartingAfter *string `query:"starting_after"`
	SortBy        *string `query:"sort_by"`
	SortDirection *string `query:"sort_direction"`
}

type PersonSearchRequest struct {
	Name          *string `json:"name" validate:"omitempty,min=1"`
	Description   *string `json:"description" validate:"omitempty"`
	Source        *string `json:"source" validate:"omitempty"`
	SinceDate     *string `json:"since_date" validate:"omitempty,datetime=2006-01-02T15:04:05Z07:00"`
	BeforeDate    *string `json:"before_date" validate:"omitempty,datetime=2006-01-02T15:04:05Z07:00"`
	Limit         *int    `json:"limit" validate:"omitempty,min=1"`
	StartingAfter *string `json:"starting_after" validate:"omitempty"`
	SortBy        *string `json:"sort_by" validate:"omitempty,oneof=relevance created_at name creator_count subject_count"`
	SortDirection *string `json:"sort_direction" validate:"omitempty,oneof=asc desc"`
}

type PersonResponse struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description *string                `json:"description,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Sources     []PersonSourceResponse `json:"sources,omitempty"`
}

func FromModel(person *models.Person) *PersonResponse {
	sources := make([]PersonSourceResponse, len(person.Sources))
	for i, src := range person.Sources {
		sources[i] = PersonSourceResponse{
			URL:         src.URL,
			Title:       src.Title,
			Description: src.Description,
		}
	}
	return &PersonResponse{
		ID:          person.UUID,
		Name:        person.Name,
		Description: person.Description,
		CreatedAt:   person.CreatedAt,
		UpdatedAt:   person.UpdatedAt,
		Sources:     sources,
	}
}

type PersonSourceRequest struct {
	URL         string  `json:"url" validate:"required,url"`
	Title       *string `json:"title,omitempty"`
	Description *string `json:"description,omitempty"`
}

type PersonSourceResponse struct {
	URL         string  `json:"url"`
	Title       *string `json:"title,omitempty"`
	Description *string `json:"description,omitempty"`
}
