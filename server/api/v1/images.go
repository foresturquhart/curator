package v1

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/labstack/echo/v4"
	"github.com/pgvector/pgvector-go"
	"github.com/rs/zerolog/log"
)

type ImageHandler struct {
	container  *container.Container
	repository *repositories.ImageRepository
}

func NewImageHandler(c *container.Container, repo *repositories.ImageRepository) *ImageHandler {
	return &ImageHandler{
		container:  c,
		repository: repo,
	}
}

// ImageTagRequest represents a tag in API requests
type ImageTagRequest struct {
	UUID string `json:"uuid"` // UUID of tag
	Name string `json:"name"` // Name of tag
}

// ImagePersonRequest represents a person in API requests
type ImagePersonRequest struct {
	ID   string            `json:"id"`   // UUID of person
	Role models.PersonRole `json:"role"` // Role of the person
}

// ImageSourceRequest represents a source in API requests
type ImageSourceRequest struct {
	URL         string  `json:"url"`         // Source URL
	Title       *string `json:"title"`       // Optional source title
	Description *string `json:"description"` // Optional source description
}

func (h *ImageHandler) CreateImage(c echo.Context) error {
	ctx := c.Request().Context()

	// Ensure the request is multipart form data
	if !strings.Contains(c.Request().Header.Get("Content-Type"), "multipart/form-data") {
		return echo.NewHTTPError(http.StatusBadRequest, "Expected multipart form data")
	}

	// Parse form
	if err := c.Request().ParseMultipartForm(32 << 20); err != nil { // 32MB max
		return echo.NewHTTPError(http.StatusBadRequest, "Error parsing form: "+err.Error())
	}

	// Get the file
	file, fileHeader, err := c.Request().FormFile("image")
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Error getting image file: "+err.Error())
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error reading file content: "+err.Error())
	}

	fileReader := bytes.NewReader(fileBytes)
	fileSize := int64(len(fileBytes))
	if fileSize < 512 {
		return echo.NewHTTPError(http.StatusBadRequest, "File too small to reliably determine content type")
	}
	buffer := fileBytes[:512]

	// Detect content type from file contents, not extension
	contentType := http.DetectContentType(buffer)

	// Map MIME types to our internal format types
	var format models.ImageFormat
	switch {
	case strings.HasPrefix(contentType, "image/jpeg"):
		format = models.FormatJPEG
	case strings.HasPrefix(contentType, "image/png"):
		format = models.FormatPNG
	case strings.HasPrefix(contentType, "image/gif"):
		format = models.FormatGIF
	default:
		return echo.NewHTTPError(http.StatusBadRequest, "Unsupported image format: "+contentType)
	}

	_, err = fileReader.Seek(0, io.SeekStart)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error processing file: "+err.Error())
	}

	// Calculate file hashes
	md5Hash, sha1Hash, err := calculateFileHashes(fileReader)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error calculating file hashes: "+err.Error())
	}

	_, err = fileReader.Seek(0, io.SeekStart)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error processing file: "+err.Error())
	}

	// TODO: stop checking for existing image here and instead do it in the Upsert function
	// We then check for the duplicate and return an error at that point
	existingFilter := models.ImageFilter{
		Hash: md5Hash,
	}

	existingImages, err := h.repository.Search(ctx, existingFilter)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error checking for duplicates: "+err.Error())
	}

	if existingImages.TotalCount > 0 {
		return echo.NewHTTPError(http.StatusConflict, "Duplicate image detected with MD5: "+md5Hash)
	}

	_, err = fileReader.Seek(0, io.SeekStart)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error processing file: "+err.Error())
	}

	// Get image dimensions
	imgConfig, _, err := image.DecodeConfig(fileReader)
	if err != nil {
		log.Error().Err(err).Msg("Error decoding image config")
		return echo.NewHTTPError(http.StatusBadRequest, "Error reading image dimensions: "+err.Error())
	}

	_, err = fileReader.Seek(0, io.SeekStart)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error processing file: "+err.Error())
	}

	// Get embedding from CLIP service
	embedding, err := h.container.Clip.GetEmbeddingFromReader(ctx, fileReader)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error getting image embedding: "+err.Error())
	}

	// Parse metadata from form
	var metadata struct {
		Title       *string              `json:"title"`
		Description *string              `json:"description"`
		Tags        []ImageTagRequest    `json:"tags"`
		People      []ImagePersonRequest `json:"people"`
		Sources     []ImageSourceRequest `json:"sources"`
	}

	if metadataJSON := c.FormValue("metadata"); metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "Invalid metadata JSON: "+err.Error())
		}
	}

	// Convert API request tags to model tags
	var tags []*models.ImageTag
	for _, tagReq := range metadata.Tags {
		if tagReq.UUID != "" || tagReq.Name != "" {
			tags = append(tags, &models.ImageTag{
				UUID: tagReq.UUID,
				Name: tagReq.Name,
			})
		}
	}

	// Convert API request people to model people
	var people []*models.ImagePerson
	for _, personReq := range metadata.People {
		if personReq.ID != "" && personReq.Role != "" {
			people = append(people, &models.ImagePerson{
				UUID: personReq.ID,
				Role: personReq.Role,
			})
		}
	}

	// Convert API request sources to model sources
	var sources []*models.ImageSource
	for _, sourceReq := range metadata.Sources {
		if sourceReq.URL != "" {
			sources = append(sources, &models.ImageSource{
				URL:         sourceReq.URL,
				Title:       sourceReq.Title,
				Description: sourceReq.Description,
			})
		}
	}

	// Wrap embedding into vector type
	imageEmbedding := pgvector.NewVector(embedding)

	// Create image model
	imageModel := &models.Image{
		Filename:    fileHeader.Filename,
		MD5:         md5Hash,
		SHA1:        sha1Hash,
		Width:       imgConfig.Width,
		Height:      imgConfig.Height,
		Format:      format,
		Size:        fileSize,
		Embedding:   &imageEmbedding,
		Title:       metadata.Title,
		Description: metadata.Description,
		Tags:        tags,
		People:      people,
		Sources:     sources,
	}

	// Store in database
	if err := h.repository.Upsert(ctx, imageModel); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error storing image: "+err.Error())
	}

	storageKey := imageModel.GetStoredName()

	_, err = fileReader.Seek(0, io.SeekStart)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error processing file: "+err.Error())
	}

	err = h.container.S3.Upload(ctx, storageKey, fileReader, imageModel.Size, contentType)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Error uploading image file: "+err.Error())
	}

	return c.JSON(http.StatusCreated, imageModel)
}

// calculateFileHashes calculates MD5 and SHA1 hashes of a file
func calculateFileHashes(reader io.Reader) (string, string, error) {
	md5Hasher := md5.New()
	sha1Hasher := sha1.New()

	teeReader := io.TeeReader(reader, io.MultiWriter(md5Hasher, sha1Hasher))

	if _, err := io.Copy(io.Discard, teeReader); err != nil {
		return "", "", err
	}

	md5Hash := hex.EncodeToString(md5Hasher.Sum(nil))
	sha1Hash := hex.EncodeToString(sha1Hasher.Sum(nil))

	return md5Hash, sha1Hash, nil
}

// applyPaginationAndSorting applies common pagination and sorting parameters to an image filter
func applyImagesPaginationAndSorting(filter *models.ImageFilter, limit *int, startingAfter *string, sortBy *string, sortDirection *string, randomSeed *string, encryptionKey string) error {
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
			filter.SortBy = models.SortByRelevance
		case "created_at":
			filter.SortBy = models.SortByCreatedAt
		case "title":
			filter.SortBy = models.SortByTitle
		case "tag_count":
			filter.SortBy = models.SortByTagCount
		case "dimensions":
			filter.SortBy = models.SortByDimensions
		case "random":
			filter.SortBy = models.SortByRandom
			if randomSeed != nil {
				filter.RandomSeed = randomSeed
			} else {
				return fmt.Errorf("seed required for random sort")
			}
		default:
			return fmt.Errorf("invalid sort_by option: %s", *sortBy)
		}
	}

	// Apply sort direction
	if sortDirection != nil {
		switch *sortDirection {
		case "asc":
			filter.SortDirection = utils.SortDirectionAsc
		case "desc":
			filter.SortDirection = utils.SortDirectionDesc
		default:
			return fmt.Errorf("invalid sort_direction option: %s", *sortDirection)
		}
	}

	return nil
}

// formatPaginatedResponse creates a standardized response with pagination info
func formatPaginatedResponse(result *models.PaginatedImageResult, encryptionKey string) (map[string]interface{}, error) {
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

type ListImagesRequest struct {
	Limit         *int    `query:"limit"`
	StartingAfter *string `query:"starting_after"`
	SortBy        *string `query:"sort_by"`
	SortDirection *string `query:"sort_direction"`
	RandomSeed    *string `query:"random_seed"`
}

func (h *ImageHandler) ListImages(c echo.Context) error {
	var req ListImagesRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data")
	}

	ctx := c.Request().Context()
	filter := models.ImageFilter{}

	// Apply pagination and sorting
	err := applyImagesPaginationAndSorting(&filter, req.Limit, req.StartingAfter,
		req.SortBy, req.SortDirection, req.RandomSeed, h.container.Config.EncryptionKey)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	// Execute search
	images, err := h.repository.Search(ctx, filter)
	if err != nil {
		log.Error().Err(err).Msg("Error listing images")
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list images")
	}

	// Format response
	response, err := formatPaginatedResponse(images, h.container.Config.EncryptionKey)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, response)
}

func (h *ImageHandler) GetImage(c echo.Context) error {
	id := c.Param("id")
	ctx := c.Request().Context()

	imageModel, err := h.repository.GetByUUID(ctx, id)
	if err != nil {
		if errors.Is(err, utils.ErrImageNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Image not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve image")
	}

	return c.JSON(http.StatusCreated, imageModel)
}

func (h *ImageHandler) UpdateImage(c echo.Context) error {
	id := c.Param("id")
	ctx := c.Request().Context()

	// Get existing image
	existingImage, err := h.repository.GetByUUID(ctx, id)
	if err != nil {
		if errors.Is(err, utils.ErrImageNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Image not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve image: "+err.Error())
	}

	// Parse update data
	var updateData struct {
		Title       *string              `json:"title"`
		Description *string              `json:"description"`
		Tags        []ImageTagRequest    `json:"tags"`
		People      []ImagePersonRequest `json:"people"`
		Sources     []ImageSourceRequest `json:"sources"`
	}

	if err := c.Bind(&updateData); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid request data: "+err.Error())
	}

	// Update only mutable fields
	if updateData.Title != nil {
		existingImage.Title = updateData.Title
	}

	if updateData.Description != nil {
		existingImage.Description = updateData.Description
	}

	// Convert API request tags to model tags
	if updateData.Tags != nil {
		var tags []*models.ImageTag
		for _, tagReq := range updateData.Tags {
			if tagReq.UUID != "" || tagReq.Name != "" {
				tags = append(tags, &models.ImageTag{
					UUID: tagReq.UUID,
					Name: tagReq.Name,
				})
			}
		}
		existingImage.Tags = tags
	}

	// Convert API request people to model people
	if updateData.People != nil {
		var people []*models.ImagePerson
		for _, personReq := range updateData.People {
			if personReq.ID != "" && personReq.Role != "" {
				people = append(people, &models.ImagePerson{
					UUID: personReq.ID,
					Role: personReq.Role,
				})
			}
		}
		existingImage.People = people
	}

	// Convert API request sources to model sources
	if updateData.Sources != nil {
		var sources []*models.ImageSource
		for _, sourceReq := range updateData.Sources {
			if sourceReq.URL != "" {
				sources = append(sources, &models.ImageSource{
					URL:         sourceReq.URL,
					Title:       sourceReq.Title,
					Description: sourceReq.Description,
				})
			}
		}
		existingImage.Sources = sources
	}

	// Save updates
	if err := h.repository.Upsert(ctx, existingImage); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to update image: "+err.Error())
	}

	return c.JSON(http.StatusOK, existingImage)
}

func (h *ImageHandler) DeleteImage(c echo.Context) error {
	id := c.Param("id")
	ctx := c.Request().Context()

	// Get the image to find its file path before deletion
	imageModel, err := h.repository.GetByUUID(ctx, id)
	if err != nil {
		if errors.Is(err, utils.ErrImageNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Image not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to retrieve image: "+err.Error())
	}

	// Determine stored file path
	storageKey := imageModel.GetStoredName()

	// Delete from database (this also handles Elasticsearch and Qdrant deletion)
	if err := h.repository.Delete(ctx, id); err != nil {
		if errors.Is(err, utils.ErrImageNotFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Image not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to delete image from database: "+err.Error())
	}

	// Delete the object from S3 storage
	if err := h.container.S3.Delete(ctx, storageKey); err != nil {
		log.Error().Err(err).Str("key", storageKey).Msg("Failed to delete image object from storage")
	}

	return c.NoContent(http.StatusNoContent)
}

type SearchImagesRequest struct {
	// Full text search
	Title       *string `query:"title"`
	Description *string `query:"description"`
	Source      *string `query:"source"`

	// Basic filtering
	Hash *string `query:"hash"`

	// Dimension filtering
	MinWidth  *int `query:"min_width"`
	MaxWidth  *int `query:"max_width"`
	MinHeight *int `query:"min_height"`
	MaxHeight *int `query:"max_height"`

	// Date filtering
	SinceDate  *string `query:"since_date"`
	BeforeDate *string `query:"before_date"`

	// Vector similarity
	SimilarToID         *string  `query:"similar_to_id"`
	SimilarityThreshold *float64 `query:"similarity_threshold"`

	// Tag filtering
	TagFilters []models.ImageTagFilter `query:"tag_filters"`

	// Person filtering
	PersonFilters []models.ImagePersonFilter `query:"person_filters"`

	// Sorting & pagination
	Limit         *int    `query:"limit"`
	StartingAfter *string `query:"starting_after"`
	SortBy        *string `query:"sort_by"`
	SortDirection *string `query:"sort_direction"`

	// Deterministic shuffle seed
	RandomSeed *string `query:"random_seed"`
}

func (h *ImageHandler) SearchImages(c echo.Context) error {
	isMultipart := c.Request().Header.Get("Content-Type") != "" &&
		strings.Contains(c.Request().Header.Get("Content-Type"), "multipart/form-data")

	var req SearchImagesRequest

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
	filter := models.ImageFilter{}

	// Apply pagination and sorting
	err := applyImagesPaginationAndSorting(&filter, req.Limit, req.StartingAfter,
		req.SortBy, req.SortDirection, req.RandomSeed, h.container.Config.EncryptionKey)

	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	// Apply basic filtering
	if req.Title != nil {
		filter.Title = *req.Title
	}

	if req.Description != nil {
		filter.Description = *req.Description
	}

	if req.Source != nil {
		filter.Source = *req.Source
	}

	if req.Hash != nil {
		filter.Hash = *req.Hash
	}

	// Apply dimension filtering
	if req.MinWidth != nil {
		filter.MinWidth = *req.MinWidth
	}

	if req.MaxWidth != nil {
		filter.MaxWidth = *req.MaxWidth
	}

	if req.MinHeight != nil {
		filter.MinHeight = *req.MinHeight
	}

	if req.MaxHeight != nil {
		filter.MaxHeight = *req.MaxHeight
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

	// Apply vector similarity
	if req.SimilarToID != nil {
		filter.SimilarToID = *req.SimilarToID
	}

	// Apply tag filters
	if len(req.TagFilters) > 0 {
		filter.TagFilters = req.TagFilters
	}

	// Apply person filters
	if len(req.PersonFilters) > 0 {
		filter.PersonFilters = req.PersonFilters
	}

	// Apply similarity threshold
	if req.SimilarityThreshold != nil {
		filter.SimilarityThreshold = *req.SimilarityThreshold
	}

	// Process file upload if present
	if isMultipart {
		file, err := c.FormFile("image")
		if err == nil { // Only process if there's an image file
			src, err := file.Open()
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, "Unable to open uploaded file")
			}
			defer src.Close()

			// Get embedding from the image file
			embedding, err := h.container.Clip.GetEmbeddingFromReader(ctx, src)
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, "Failed to get image embedding")
			}
			vecEmbedding := pgvector.NewVector(embedding)
			filter.SimilarToEmbedding = &vecEmbedding

			// Force sort by similarity
			filter.SortBy = models.SortByRelevance
			filter.SortDirection = utils.SortDirectionDesc
		}
	}

	// Execute search
	images, err := h.repository.Search(ctx, filter)
	if err != nil {
		log.Error().Err(err).Msg("Error searching images")
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to search images")
	}

	// Format response
	response, err := formatPaginatedResponse(images, h.container.Config.EncryptionKey)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, response)
}
