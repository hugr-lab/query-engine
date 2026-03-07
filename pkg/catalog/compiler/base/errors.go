package base

import (
	"errors"
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var (
	ErrDefinitionNotFound = errors.New("definition not found")
)

func ErrorPosf(pos *ast.Position, format string, args ...any) *gqlerror.Error {
	if pos == nil || pos.Src == nil {
		return gqlerror.Wrap(fmt.Errorf(format, args...))
	}
	return gqlerror.ErrorPosf(pos, format, args...)
}
