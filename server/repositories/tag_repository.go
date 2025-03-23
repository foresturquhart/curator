package repositories

import (
	"github.com/foresturquhart/curator/server/container"
)

type TagRepository struct {
	container *container.Container
}

func NewTagRepository(container *container.Container) *TagRepository {
	return &TagRepository{
		container: container,
	}
}
