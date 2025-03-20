package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	v1 "github.com/foresturquhart/curator/server/api/v1"
	"github.com/foresturquhart/curator/server/config"
	"github.com/foresturquhart/curator/server/container"
	"github.com/foresturquhart/curator/server/repositories"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
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
	c, err := container.NewContainer(cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize application container")
	}
	defer c.Close()

	// Perform migrations
	if err := c.Migrate(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("Failed to perform migrations")
	}

	// Initialize repositories
	imageRepository := repositories.NewImageRepository(c)
	personRepository := repositories.NewPersonRepository(c)
	// tagRepository := repositories.NewTagRepository(c)
	// collectionRepository := repositories.NewCollectionRepository(c)

	if err := imageRepository.ReindexAll(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("Failed to reindex images")
	}
	if err := personRepository.ReindexAll(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("Failed to reindex people")
	}
	// if err := tagRepository.ReindexAll(context.Background()); err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to reindex tags")
	// }
	// if err := collectionRepository.ReindexAll(context.Background()); err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to reindex collections")
	// }

	// Set up Echo server
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	// Register API routes
	v1.RegisterImageRoutes(e, c, imageRepository)
	v1.RegisterPersonRoutes(e, c, personRepository)
	// v1.RegisterTagRoutes(e, c, tagRepository)
	// v1.RegisterCollectionRoutes(e, c, collectionRepository)

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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := e.Shutdown(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to gracefully shutdown server")
	}
}
