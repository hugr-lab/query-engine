package schema

import (
	"fmt"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

// MarshalType serializes an *ast.Type to its canonical GraphQL string
// representation (e.g., "[String!]!", "Int", "Boolean!").
func MarshalType(t *ast.Type) string {
	if t == nil {
		return ""
	}
	return t.String()
}

// UnmarshalType parses a GraphQL type string back into an *ast.Type.
// Returns an error for malformed input.
func UnmarshalType(s string) (*ast.Type, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty type string")
	}
	t, rest, err := parseType(s)
	if err != nil {
		return nil, err
	}
	if rest != "" {
		return nil, fmt.Errorf("unexpected trailing characters in type string: %q", rest)
	}
	return t, nil
}

// parseType recursively parses a GraphQL type string, returning the parsed type
// and any remaining unparsed characters.
func parseType(s string) (*ast.Type, string, error) {
	if s == "" {
		return nil, "", fmt.Errorf("unexpected end of type string")
	}

	if s[0] == '[' {
		// List type: [InnerType]!?
		inner, rest, err := parseType(s[1:])
		if err != nil {
			return nil, "", fmt.Errorf("in list type: %w", err)
		}
		if rest == "" || rest[0] != ']' {
			return nil, "", fmt.Errorf("missing closing ']' in list type")
		}
		rest = rest[1:]

		t := &ast.Type{Elem: inner}
		if len(rest) > 0 && rest[0] == '!' {
			t.NonNull = true
			rest = rest[1:]
		}
		return t, rest, nil
	}

	// Named type: Name!?
	i := 0
	for i < len(s) && s[i] != '!' && s[i] != ']' && s[i] != '[' {
		i++
	}
	if i == 0 {
		return nil, "", fmt.Errorf("unexpected character %q in type string", s[0])
	}
	name := s[:i]
	rest := s[i:]

	t := &ast.Type{NamedType: name}
	if len(rest) > 0 && rest[0] == '!' {
		t.NonNull = true
		rest = rest[1:]
	}
	return t, rest, nil
}
