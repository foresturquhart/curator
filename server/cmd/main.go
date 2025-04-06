package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/davecgh/go-spew/spew"
	v1 "github.com/foresturquhart/curator/server/api/v1"
	"github.com/foresturquhart/curator/server/config"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/models"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/foresturquhart/curator/server/services"
	"github.com/foresturquhart/curator/server/utils"
	"github.com/foresturquhart/curator/server/worker"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx := context.Background()

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Configure logging
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if lvl, err := zerolog.ParseLevel(cfg.LogLevel); err == nil {
		zerolog.SetGlobalLevel(lvl)
	}

	// Initialize container with all dependencies
	c, err := container.NewContainer(ctx, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize application container")
	}
	defer c.Close()

	// Perform migrations
	if err := c.Migrate(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to perform migrations")
	}

	// Initialize repositories
	imageRepository := repositories.NewImageRepository(c)
	// collectionRepository := repositories.NewCollectionRepository(c)

	// Initialize services
	personService := services.NewPersonService(c)
	tagService := services.NewTagService(c)

	if err := imageRepository.IndexAll(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to reindex images")
	}
	if err := personService.IndexAll(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to reindex people")
	}
	if err := tagService.IndexAll(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to reindex tags")
	}
	// if err := collectionRepository.ReindexAll(ctx); err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to reindex collections")
	// }

	// Initialize worker
	worker, err := worker.NewWorker(c, imageRepository, personService, tagService)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize background worker")
	}

	c.Worker = worker

	// Start the worker in a goroutine
	go func() {
		if err := worker.Start(); err != nil {
			log.Error().Err(err).Msg("Failed to start background worker")
		}
	}()

	tag := &models.Tag{
		Name:        "Finches",
		Description: utils.NewPointer("Finches are little birds."),
	}

	err = tagService.Create(ctx, tag, repositories.TagCreateOptions{
		Action:   repositories.TagHierarchyBefore,
		TargetID: utils.NewPointer(int64(5)),
	})
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create tag")
	}
	spew.Dump(tag)

	// Set up Echo server
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	// Register API routes
	v1.RegisterRoutes(e, c, imageRepository, personService)

	// Start the server
	go func() {
		log.Info().Msgf("Starting the server on :%d", cfg.Port)
		if err := e.Start(fmt.Sprintf(":%d", cfg.Port)); err != nil {
			log.Info().Msg("Shutting down the server")
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Stop the worker gracefully
	if err := worker.Stop(); err != nil {
		log.Error().Err(err).Msg("Failed to gracefully stop background worker")
	}

	// Stop the server gracefully
	if err := e.Shutdown(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to gracefully shutdown server")
	}
}
