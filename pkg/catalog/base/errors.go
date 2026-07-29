package base

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ErrDefinitionNotFound went with the static provider's write surface — the
// setters and Drop* methods that returned it. A missing definition on the read
// path is a nil, not an error.

func ErrorPosf(pos *ast.Position, format string, args ...any) *gqlerror.Error {
	if pos == nil || pos.Src == nil {
		return gqlerror.Wrap(fmt.Errorf(format, args...))
	}
	return gqlerror.ErrorPosf(pos, format, args...)
}
