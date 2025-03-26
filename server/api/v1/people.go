package v1

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/services"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

type PersonHandler struct {
	container *container.Container
	service   *services.PersonService
}

func NewPersonHandler(c *container.Container, svc *services.PersonService) *PersonHandler {
	return &PersonHandler{
		container: c,
		service:   svc,
	}
}

func RegisterPersonRoutes(e *echo.Echo, c *container.Container, svc *services.PersonService) {
	handler := NewPersonHandler(c, svc)

	v1 := e.Group("/v1")
	people := v1.Group("/people")

	// Create
	people.POST("", handler.CreatePerson)
	people.GET("", handler.ListPeople)
	people.GET("/:uuid", handler.GetPerson)
	people.PUT("/:uuid", handler.UpdatePerson)
	people.DELETE("/:uuid", handler.DeletePerson)
	people.POST("/search", handler.SearchPeople)
}

// PersonSourceRequest represents a source in API requests
type PersonSourceRequest struct {
	URL         string  `json:"url"`         // Source URL
	Title       *string `json:"title"`       // Optional source title
	Description *string `json:"description"` // Optional source description
}

func (h *PersonHandler) CreatePerson(c echo.Context) error {
	ctx := c.Request().Context()

	// Parse request body
	var request struct {
		Name        string                `json:"name"`
		Description *string               `json:"description"`
		Sources     []PersonSourceRequest `json:"sources"`
	}

	if err := c.Bind(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data: "+err.Error())
	}

	// Validate required fields
	if request.Name == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Name is required")
	}

	// Convert API request sources to model sources
	var sources []*models.PersonSource
	for _, sourceReq := range request.Sources {
		if sourceReq.URL != "" {
			sources = append(sources, &models.PersonSource{
				URL:         sourceReq.URL,
				Title:       sourceReq.Title,
				Description: sourceReq.Description,
			})
		}
	}

	// Create person model
	personModel := &models.Person{
		Name:        request.Name,
		Description: request.Description,
		Sources:     sources,
	}

	// Store in database
	if err := h.service.Create(ctx, personModel); err != nil {
		if conflictErr, ok := err.(*utils.ConflictError); ok {
			return c.JSON(http.StatusConflict, map[string]interface{}{
				"error":       "A person with this name already exists",
				"conflict_id": conflictErr.ConflictUUID,
			})
		}

		return echo.NewHTTPError(http.StatusInternalServerError, "Error storing person: "+err.Error())
	}

	return c.JSON(http.StatusCreated, personModel)
}

// applyPaginationAndSorting applies common pagination and sorting parameters to a person filter
func applyPeoplePaginationAndSorting(filter *models.PersonFilter, limit *int, startingAfter *string, sortBy *string, sortDirection *string, encryptionKey string) error {
	// Apply limit
	if limit != nil {
		filter.Limit = *limit
	}

	// Apply cursor
	if startingAfter != nil {
		cursor, err := utils.DecryptCursor(*startingAfter, encryptionKey)
		if err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
		filter.StartingAfter = cursor
	}

	// Apply sort field
	if sortBy != nil {
		switch *sortBy {
		case "relevance":
			filter.SortBy = models.PersonSortByRelevance
		case "created_at":
			filter.SortBy = models.PersonSortByCreatedAt
		case "name":
			filter.SortBy = models.PersonSortByName
		case "creator_count":
			filter.SortBy = models.PersonSortByCreatorCount
		case "subject_count":
			filter.SortBy = models.PersonSortBySubjectCount
		default:
			return fmt.Errorf("invalid sort_by option: %s", *sortBy)
		}
	}

	// Apply sort direction
	if sortDirection != nil {
		switch *sortDirection {
		case "asc":
			filter.SortDirection = models.SortDirectionAsc
		case "desc":
			filter.SortDirection = models.SortDirectionDesc
		default:
			return fmt.Errorf("invalid sort_direction option: %s", *sortDirection)
		}
	}

	return nil
}

// formatPaginatedResponse creates a standardized response with pagination info
func formatPaginatedPersonResponse(result *models.PaginatedPersonResult, encryptionKey string) (map[string]interface{}, error) {
	response := map[string]interface{}{
		"data":        result.Data,
		"has_more":    result.HasMore,
		"total_count": result.TotalCount,
	}

	if result.NextCursor != nil {
		cursor, err := utils.EncryptCursor(result.NextCursor, encryptionKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt cursor: %w", err)
		}
		response["next_cursor"] = cursor
	}

	return response, nil
}

type ListPeopleRequest struct {
	Limit         *int    `query:"limit"`
	StartingAfter *string `query:"starting_after"`
	SortBy        *string `query:"sort_by"`
	SortDirection *string `query:"sort_direction"`
}

func (h *PersonHandler) ListPeople(c echo.Context) error {
	var req ListPeopleRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data")
	}

	ctx := c.Request().Context()
	filter := &models.PersonFilter{}

	// Apply pagination and sorting
	err := applyPeoplePaginationAndSorting(filter, req.Limit, req.StartingAfter,
		req.SortBy, req.SortDirection, h.container.Config.EncryptionKey)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	// Execute search
	people, err := h.service.Search(ctx, filter)
	if err != nil {
		log.Error().Err(err).Msg("Error listing people")
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list people")
	}

	// Format response
	response, err := formatPaginatedPersonResponse(people, h.container.Config.EncryptionKey)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, response)
}

func (h *PersonHandler) GetPerson(c echo.Context) error {
	uuid := c.Param("uuid")
	ctx := c.Request().Context()

	personModel, err := h.service.Get(ctx, uuid)
	if err != nil {
		if errors.Is(err, utils.ErrPersonNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Person not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve person")
	}

	return c.JSON(http.StatusOK, personModel)
}

func (h *PersonHandler) UpdatePerson(c echo.Context) error {
	uuid := c.Param("uuid")
	ctx := c.Request().Context()

	// Get existing person
	existingPerson, err := h.service.Get(ctx, uuid)
	if err != nil {
		if errors.Is(err, utils.ErrPersonNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Person not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve person: "+err.Error())
	}

	// Parse update data
	var updateData struct {
		Name        *string               `json:"name"`
		Description *string               `json:"description"`
		Sources     []PersonSourceRequest `json:"sources"`
	}

	if err := c.Bind(&updateData); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data: "+err.Error())
	}

	// Update only mutable fields
	if updateData.Name != nil {
		existingPerson.Name = *updateData.Name
	}

	if updateData.Description != nil {
		existingPerson.Description = updateData.Description
	}

	// Convert API request sources to model sources
	if updateData.Sources != nil {
		var sources []*models.PersonSource
		for _, sourceReq := range updateData.Sources {
			if sourceReq.URL != "" {
				sources = append(sources, &models.PersonSource{
					URL:         sourceReq.URL,
					Title:       sourceReq.Title,
					Description: sourceReq.Description,
				})
			}
		}
		existingPerson.Sources = sources
	}

	// Save updates
	if err := h.service.Update(ctx, existingPerson); err != nil {
		if conflictErr, ok := err.(*utils.ConflictError); ok {
			return c.JSON(http.StatusConflict, map[string]interface{}{
				"error":       "A person with this name already exists",
				"conflict_id": conflictErr.ConflictUUID,
			})
		}

		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to update person: "+err.Error())
	}

	return c.JSON(http.StatusOK, existingPerson)
}

func (h *PersonHandler) DeletePerson(c echo.Context) error {
	uuid := c.Param("uuid")
	ctx := c.Request().Context()

	// Delete from database
	if err := h.service.Delete(ctx, uuid); err != nil {
		if errors.Is(err, utils.ErrPersonNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Person not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to delete person: "+err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

type SearchPeopleRequest struct {
	// Full text search
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Source      *string `json:"source"`

	// Date filtering
	SinceDate  *string `json:"since_date"`
	BeforeDate *string `json:"before_date"`

	// Sorting & pagination
	Limit         *int    `json:"limit"`
	StartingAfter *string `json:"starting_after"`
	SortBy        *string `json:"sort_by"`
	SortDirection *string `json:"sort_direction"`
}

func (h *PersonHandler) SearchPeople(c echo.Context) error {
	isMultipart := c.Request().Header.Get("Content-Type") != "" &&
		strings.Contains(c.Request().Header.Get("Content-Type"), "multipart/form-data")

	var req SearchPeopleRequest

	// If it's a multipart form, extract the JSON from the "data" field manually.
	if isMultipart {
		jsonData := c.FormValue("data")
		if jsonData == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "Missing JSON data in form")
		}
		if err := json.Unmarshal([]byte(jsonData), &req); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid JSON in form data")
		}
	} else {
		// Fallback to automatic binding for non-multipart requests.
		if err := c.Bind(&req); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data")
		}
	}

	ctx := c.Request().Context()

	// Build filter from request
	filter := &models.PersonFilter{}

	// Apply pagination and sorting
	err := applyPeoplePaginationAndSorting(filter, req.Limit, req.StartingAfter,
		req.SortBy, req.SortDirection, h.container.Config.EncryptionKey)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	// Apply basic filtering
	if req.Name != nil {
		filter.Name = *req.Name
	}

	if req.Description != nil {
		filter.Description = *req.Description
	}

	if req.Source != nil {
		filter.Source = *req.Source
	}

	// Apply date filtering
	if req.SinceDate != nil {
		// Parse time from string
		sinceTime, err := time.Parse(time.RFC3339, *req.SinceDate)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid since_date format, expected RFC3339")
		}
		filter.SinceDate = &sinceTime
	}

	if req.BeforeDate != nil {
		// Parse time from string
		beforeTime, err := time.Parse(time.RFC3339, *req.BeforeDate)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid before_date format, expected RFC3339")
		}
		filter.BeforeDate = &beforeTime
	}

	// Execute search
	people, err := h.service.Search(ctx, filter)
	if err != nil {
		log.Error().Err(err).Msg("Error searching people")
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to search people")
	}

	// Format response
	response, err := formatPaginatedPersonResponse(people, h.container.Config.EncryptionKey)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, response)
}
