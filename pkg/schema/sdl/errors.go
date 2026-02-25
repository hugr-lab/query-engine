package sdl

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func ErrorPosf(pos *ast.Position, format string, args ...interface{}) *gqlerror.Error {
	if pos == nil || pos.Src == nil {
		return gqlerror.Wrap(fmt.Errorf(format, args...))
	}
	return gqlerror.ErrorPosf(pos, format, args...)
}
