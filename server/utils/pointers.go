package utils

// NewPointer returns a pointer to the object passed.
func NewPointer[T any](t T) *T { return &t }
