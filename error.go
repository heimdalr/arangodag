package arangodag

import (
	"errors"
	"fmt"
)

// Error constants
const (
	ErrVertexNil      = 1101

	ErrEmptyID      = 1201
	ErrDuplicateKey = 1202
	ErrUnknownID    = 1203

	//ErrDuplicateEdge  = 1301
	//ErrUnknownEdge    = 1301
	//ErrLoop           = 1302
	//ErrSrcDstEqual    = 1303

	ErrArango         = 1401
)


// Error is the type for DAG errors.
type Error struct {
	IsDAGError   bool   `json:"isDAGError"`
	ErrorNum     int    `json:"errorNum"`
	ErrorMessage string `json:"errorMessage"`
	Err          error  `json:"error"`
}

// NewError returns a new DAG error.
func NewError(num int, format string, args ...interface{}) Error {
	return Error{
		IsDAGError:   true,
		ErrorNum:     num,
		ErrorMessage: fmt.Sprintf(format, args...),
	}
}

// Implements the error interface.
func (e Error) Error() string {
	if e.ErrorMessage != "" {
		return e.ErrorMessage
	}
	if e.ErrorNum == ErrArango {
		return fmt.Sprintf("Arango Error: %v", e.Err)
	}
	return fmt.Sprintf("Error: ErrorNum %d", e.ErrorNum)
}

// Unwrap supports unwrapping of errors.
func (e Error) Unwrap() error {
	return e.Err
}

// Is provides for correct comparison of DAG errors using the errors.Is() method.
// (see: https://pkg.go.dev/errors).
func (e Error) Is(target error) bool {
	t, ok := target.(Error)
	if !ok {
		return false
	}
	return e.ErrorNum == t.ErrorNum && t.IsDAGError == true
}

// IsErrorWithErrorNum returns true, if the given error is a DAG
// error with an error number equal to the given one.
func IsErrorWithErrorNum(err error, num int) bool {
	return errors.Is(err, NewError(num, ""))
}


// VertexNilError creates a new DAG error with an error number equal to
// ErrVertexNil and an appropriate error message.
func VertexNilError() Error {
	return NewError(ErrVertexNil, "don't know what to do with 'nil'")
}

// IsVertexNilError returns true, if the given error is a DAG error
// with an error number equal to ErrVertexNil.
func IsVertexNilError(err error) bool {
	return IsErrorWithErrorNum(err, ErrVertexNil)
}

// DuplicateIDError creates a new DAG error with an error number equal to
// ErrDuplicateKey and an appropriate error message.
func DuplicateIDError(id string) Error {
	return NewError(ErrDuplicateKey, "'%s' is already known", id)
}

// IsDuplicateIDError returns true, if the given error is a DAG error
// with an error number equal to ErrDuplicateKey.
func IsDuplicateIDError(err error) bool {
	return IsErrorWithErrorNum(err, ErrDuplicateKey)
}

// EmptyIDError creates a new DAG error with an error number equal to
// ErrEmptyID and an appropriate error message.
func EmptyIDError() Error {
	return NewError(ErrEmptyID, "id is empty")
}

// IsEmptyKeyError returns true, if the given error is a DAG error
// with an error number equal to ErrEmptyID.
func IsEmptyKeyError(err error) bool {
	return IsErrorWithErrorNum(err, ErrEmptyID)
}


// IsUnknownKeyError returns true, if the given error is a DAG error
// with an error number equal to ErrUnknownID.
func IsUnknownKeyError(err error) bool {
	return IsErrorWithErrorNum(err, ErrUnknownID)
}

// NewUnknownKeyError creates a new DAG error with an error number equal to
// ErrUnknownID and an appropriate error message.
func NewUnknownKeyError(key string) Error {
	return NewError(ErrUnknownID, "'%s' is unknown", key)
}


/*



// IsLoopError returns true, if the given error is a DAG error
// with an error number equal to ErrLoop.
func IsLoopError(err error) bool {
	return IsErrorWithErrorNum(err, ErrLoop)
}

// IsDuplicateEdgeError returns true, if the given error is a DAG error
// with an error number equal to ErrDuplicateEdge.
func IsDuplicateEdgeError(err error) bool {
	return IsErrorWithErrorNum(err, ErrDuplicateEdge)
}

// IsUnknownEdgeError returns true, if the given error is a DAG error
// with an error number equal to ErrUnknownEdge.
func IsUnknownEdgeError(err error) bool {
	return IsErrorWithErrorNum(err, ErrUnknownEdge)
}

// IsSrcDstEqualError returns true, if the given error is a DAG error
// with an error number equal to ErrSrcDstEqual.
func IsSrcDstEqualError(err error) bool {
	return IsErrorWithErrorNum(err, ErrSrcDstEqual)
}




// NewDuplicateEdgeError creates a new DAG error with an error number equal to
// ErrDuplicateEdge and an appropriate error message.
func NewDuplicateEdgeError(src string, dst string) Error {
	return NewError(ErrDuplicateEdge, "edge between '%s' and '%s' is already known", src, dst)
}

// NewUnknownEdgeError creates a new DAG error with an error number equal to
// ErrUnknownEdge and an appropriate error message.
func NewUnknownEdgeError(src string, dst string) Error {
	return NewError(ErrUnknownEdge, "edge between '%s' and '%s' is unknown", src, dst)
}

// NewLoopError creates a new DAG error with an error number equal to
// ErrLoop and an appropriate error message.
func NewLoopError(src string, dst string) Error {
	return NewError(ErrLoop, "edge between '%s' and '%s' would create a loop", src, dst)
}

// NewSrcDstEqualError creates a new DAG error with an error number equal to
// ErrSrcDstEqual and an appropriate error message.
func NewSrcDstEqualError(key string) Error {
	return NewError(ErrSrcDstEqual, "source and destination are equal ('%s')", key)
}


*/