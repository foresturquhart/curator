package utils

import (
	"errors"
	"fmt"
)

var (
	ErrImageNotFound  = errors.New("image not found")
	ErrPersonNotFound = errors.New("person not found")
	ErrTagNotFound    = errors.New("tag not found")

	ErrInvalidInput = errors.New("invalid input")
)

// ConflictError represents a conflict with an existing resource
type ConflictError struct {
	Message      string
	ConflictUUID string
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("Conflict: %s (UUID: %s)", e.Message, e.ConflictUUID)
}
