package utils

import "errors"

var (
	ErrImageNotFound  = errors.New("image not found")
	ErrPersonNotFound = errors.New("person not found")
	ErrTagNotFound    = errors.New("tag not found")

	ErrInvalidInput = errors.New("invalid input")
)
