package arangodag

import (
	"errors"
	"fmt"
	"github.com/arangodb/go-driver"
)

// Error constants
const (
	ErrVertexNil = 1101

	ErrEmptyID     = 1201
	ErrDuplicateID = 1202
	ErrUnknownID   = 1203

	ErrDuplicateEdge = 1301
	ErrUnknownEdge   = 1301
	ErrLoop          = 1302
	ErrSrcDstEqual   = 1303

	ErrArango = 1401
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
	return e.ErrorNum == t.ErrorNum && t.IsDAGError
}

// IsErrorWithErrorNum returns true, if the given error is a DAG error with an
// error number equal to the given one.
func IsErrorWithErrorNum(err error, num int) bool {
	return errors.Is(err, NewError(num, ""))
}

// VertexNilError creates a new DAG error with an error number equal to
// ErrVertexNil and an appropriate error message.
func VertexNilError() Error {
	return NewError(ErrVertexNil, "don't know what to do with 'nil'")
}

// IsVertexNilError returns true, if the given error is a DAG error with an error
// number equal to ErrVertexNil.
func IsVertexNilError(err error) bool {
	return IsErrorWithErrorNum(err, ErrVertexNil)
}

// DuplicateIDError creates a new DAG error with an error number equal to
// ErrDuplicateID and an appropriate error message.
func DuplicateIDError(id string) Error {
	return NewError(ErrDuplicateID, "'%s' is already known", id)
}

// IsDuplicateIDError returns true, if the given error is a DAG error with an
// error number equal to ErrDuplicateID.
func IsDuplicateIDError(err error) bool {
	return IsErrorWithErrorNum(err, ErrDuplicateID)
}

// EmptyIDError creates a new DAG error with an error number equal to ErrEmptyID
// and an appropriate error message.
func EmptyIDError() Error {
	return NewError(ErrEmptyID, "id is empty")
}

// IsEmptyIDError returns true, if the given error is a DAG error with an error
// number equal to ErrEmptyID.
func IsEmptyIDError(err error) bool {
	return IsErrorWithErrorNum(err, ErrEmptyID)
}

// UnknownIDError creates a new DAG error with an error number equal to
// ErrUnknownID and an appropriate error message.
func UnknownIDError(id string) Error {
	return NewError(ErrUnknownID, "'%s' is unknown", id)
}

// IsUnknownIDError returns true, if the given error is a DAG error with an error
// number equal to ErrUnknownID.
func IsUnknownIDError(err error) bool {
	return IsErrorWithErrorNum(err, ErrUnknownID)
}

// ArangoError creates a new DAG error with an error number equal to ErrArango
// and an appropriate error message. The Arango error is thereby wrapped.
func ArangoError(err error) Error {
	e := NewError(ErrArango, "Arango error")
	e.Err = err
	return e
}

// IsAragoError returns true, if the given error is a DAG error with an error
// number equal to ErrArango.
func IsAragoError(err error) bool {
	return IsErrorWithErrorNum(err, ErrArango)
}

// IsAragoErrorWithErrorNum returns true, if the given error is a DAG error with
// an error number equal to ErrArango and the wrapped error is an Arango error
// with the an error number equal to the given num.
func IsAragoErrorWithErrorNum(err error, num int) bool {
	t, ok := err.(Error)
	if !ok {
		return false
	}
	return t.ErrorNum == ErrArango && driver.IsArangoErrorWithErrorNum(t.Err, num)
}

// SrcDstEqualError creates a new DAG error with an error number equal to
// ErrSrcDstEqual and an appropriate error message.
func SrcDstEqualError(id string) Error {
	return NewError(ErrSrcDstEqual, "source and destination are equal ('%s')", id)
}

// IsSrcDstEqualError returns true, if the given error is a DAG error with an
// error number equal to ErrSrcDstEqual.
func IsSrcDstEqualError(err error) bool {
	return IsErrorWithErrorNum(err, ErrSrcDstEqual)
}

// DuplicateEdgeError creates a new DAG error with an error number equal to
// ErrDuplicateEdge and an appropriate error message.
func DuplicateEdgeError(src string, dst string) Error {
	return NewError(ErrDuplicateEdge, "edge between '%s' and '%s' is already known", src, dst)
}

// IsDuplicateEdgeError returns true, if the given error is a DAG error with an
// error number equal to ErrDuplicateEdge.
func IsDuplicateEdgeError(err error) bool {
	return IsErrorWithErrorNum(err, ErrDuplicateEdge)
}

// UnknownEdgeError creates a new DAG error with an error number equal to
// ErrUnknownEdge and an appropriate error message.
func UnknownEdgeError(src string, dst string) Error {
	return NewError(ErrUnknownEdge, "edge between '%s' and '%s' is unknown", src, dst)
}

// IsUnknownEdgeError returns true, if the given error is a DAG error with an
// error number equal to ErrUnknownEdge.
func IsUnknownEdgeError(err error) bool {
	return IsErrorWithErrorNum(err, ErrUnknownEdge)
}

// LoopError creates a new DAG error with an error number equal to ErrLoop and an
// appropriate error message.
func LoopError(src string, dst string) Error {
	return NewError(ErrLoop, "edge between '%s' and '%s' would create a loop", src, dst)
}

// IsLoopError returns true, if the given error is a DAG error with an error
// number equal to ErrLoop.
func IsLoopError(err error) bool {
	return IsErrorWithErrorNum(err, ErrLoop)
}
