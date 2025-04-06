package utils

// NewPointer returns a pointer to the object passed.
func NewPointer[T any](t T) *T { return &t }

// ValueOrEmpty returns a default value if the pointer is nil, or the result of applying the function
// to the pointer value if it's not nil.
func ValueOrEmpty[T any, R any](ptr *T, fn func(t *T) R) R {
	var zero R
	if ptr == nil {
		return zero
	}
	return fn(ptr)
}
