package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/foresturquhart/curator/server/api/v1/dtos"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/search"
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

func (h *PersonHandler) CreatePerson(c echo.Context) error {
	ctx := c.Request().Context()

	var req dtos.PersonCreateRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid request data: %v", err))
	}
	if err := dtos.Validate.Struct(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Validation error: %v", err))
	}

	person := req.ToModel()
	if err := h.service.Create(ctx, person); err != nil {
		if conflictErr, ok := err.(*utils.ConflictError); ok {
			return c.JSON(http.StatusConflict, map[string]interface{}{
				"error":       "A person with this name already exists",
				"conflict_id": conflictErr.ConflictUUID,
			})
		}
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Error storing person: %v", err))
	}

	return c.JSON(http.StatusCreated, dtos.FromModel(person))
}

func (h *PersonHandler) ListPeople(c echo.Context) error {
	ctx := c.Request().Context()

	var req dtos.PersonListRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request parameters")
	}
	if err := dtos.Validate.Struct(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Validation error: %v", err))
	}

	options := &search.PersonSearchOptions{}
	if err := applyPeoplePaginationAndSorting(options, req.Limit, req.StartingAfter, req.SortBy, req.SortDirection, h.container.Config.EncryptionKey); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	people, err := h.service.Search(ctx, options)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list people")
	}

	response, err := formatPaginatedPersonResponse(people, h.container.Config.EncryptionKey)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, response)
}

func (h *PersonHandler) GetPerson(c echo.Context) error {
	ctx := c.Request().Context()
	uuid := c.Param("uuid")

	person, err := h.service.Get(ctx, uuid)
	if err != nil {
		if errors.Is(err, utils.ErrPersonNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Person not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve person")
	}

	return c.JSON(http.StatusOK, dtos.FromModel(person))
}

func (h *PersonHandler) UpdatePerson(c echo.Context) error {
	ctx := c.Request().Context()
	uuid := c.Param("uuid")

	existingPerson, err := h.service.Get(ctx, uuid)
	if err != nil {
		if errors.Is(err, utils.ErrPersonNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Person not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to retrieve person: %v", err))
	}

	var req dtos.PersonUpdateRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid request data: %v", err))
	}
	if err := dtos.Validate.Struct(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Validation error: %v", err))
	}

	req.UpdateModel(existingPerson)
	if err := h.service.Update(ctx, existingPerson); err != nil {
		if conflictErr, ok := err.(*utils.ConflictError); ok {
			return c.JSON(http.StatusConflict, map[string]interface{}{
				"error":       "A person with this name already exists",
				"conflict_id": conflictErr.ConflictUUID,
			})
		}
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to update person: %v", err))
	}

	return c.JSON(http.StatusOK, dtos.FromModel(existingPerson))
}

func (h *PersonHandler) DeletePerson(c echo.Context) error {
	ctx := c.Request().Context()
	uuid := c.Param("uuid")

	if err := h.service.Delete(ctx, uuid); err != nil {
		if errors.Is(err, utils.ErrPersonNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Person not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, fmt.Sprintf("Failed to delete person: %v", err))
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *PersonHandler) SearchPeople(c echo.Context) error {
	ctx := c.Request().Context()

	var req dtos.PersonSearchRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data")
	}
	if err := dtos.Validate.Struct(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Validation error: %v", err))
	}

	options := &search.PersonSearchOptions{}
	if err := applyPeoplePaginationAndSorting(options, req.Limit, req.StartingAfter, req.SortBy, req.SortDirection, h.container.Config.EncryptionKey); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	// Apply pagination and sorting
	err := applyPeoplePaginationAndSorting(options, req.Limit, req.StartingAfter,
		req.SortBy, req.SortDirection, h.container.Config.EncryptionKey)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	if req.Name != nil {
		options.Name = *req.Name
	}
	if req.Description != nil {
		options.Description = *req.Description
	}
	if req.Source != nil {
		options.Source = *req.Source
	}
	if req.SinceDate != nil {
		sinceTime, err := time.Parse(time.RFC3339, *req.SinceDate)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid since_date format, expected RFC3339")
		}
		options.SinceDate = &sinceTime
	}
	if req.BeforeDate != nil {
		beforeTime, err := time.Parse(time.RFC3339, *req.BeforeDate)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid before_date format, expected RFC3339")
		}
		options.BeforeDate = &beforeTime
	}

	people, err := h.service.Search(ctx, options)
	if err != nil {
		log.Error().Err(err).Msg("Error searching people")
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to search people")
	}

	response, err := formatPaginatedPersonResponse(people, h.container.Config.EncryptionKey)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, response)
}

func applyPeoplePaginationAndSorting(options *search.PersonSearchOptions, limit *int, startingAfter *string, sortBy *string, sortDirection *string, encryptionKey string) error {
	if limit != nil {
		options.Limit = *limit
	}

	if startingAfter != nil {
		cursor, err := utils.DecryptCursor(*startingAfter, encryptionKey)
		if err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
		options.StartingAfter = cursor
	}

	if sortBy != nil {
		switch *sortBy {
		case "relevance":
			options.SortBy = search.PersonSortByRelevance
		case "created_at":
			options.SortBy = search.PersonSortByCreatedAt
		case "name":
			options.SortBy = search.PersonSortByName
		default:
			return fmt.Errorf("invalid sort_by option: %s", *sortBy)
		}
	}

	if sortDirection != nil {
		switch *sortDirection {
		case "asc":
			options.SortDirection = utils.SortDirectionAsc
		case "desc":
			options.SortDirection = utils.SortDirectionDesc
		default:
			return fmt.Errorf("invalid sort_direction option: %s", *sortDirection)
		}
	}

	return nil
}

func formatPaginatedPersonResponse(result *utils.PaginatedResult[*models.Person], encryptionKey string) (map[string]interface{}, error) {
	response := map[string]any{
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
