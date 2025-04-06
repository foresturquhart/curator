package v1

import (
	"github.com/foresturquhart/curator/server/api/v1/handlers"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/services"
	"github.com/labstack/echo/v4"
)

func registerImageRoutes(g *echo.Group, c *container.Container, repo *repositories.ImageRepository) {
	handler := NewImageHandler(c, repo)

	images := g.Group("/images")

	// Create
	images.POST("", handler.CreateImage)
	images.GET("", handler.ListImages)
	images.GET("/:id", handler.GetImage)
	images.PUT("/:id", handler.UpdateImage)
	images.DELETE("/:id", handler.DeleteImage)
	images.POST("/search", handler.SearchImages)
}

func registerPersonRoutes(g *echo.Group, c *container.Container, svc *services.PersonService) {
	handler := handlers.NewPersonHandler(c, svc)

	people := g.Group("/people")

	// Create
	people.POST("", handler.CreatePerson)
	people.GET("", handler.ListPeople)
	people.GET("/:uuid", handler.GetPerson)
	people.PUT("/:uuid", handler.UpdatePerson)
	people.DELETE("/:uuid", handler.DeletePerson)
	people.POST("/search", handler.SearchPeople)
}

func RegisterRoutes(e *echo.Echo, c *container.Container, repo *repositories.ImageRepository, svc *services.PersonService) {
	group := e.Group("/v1")

	registerImageRoutes(group, c, repo)
	registerPersonRoutes(group, c, svc)
}
