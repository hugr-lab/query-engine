package store

import (
	"context"
)

// NameExists reports whether ForName would serve the name — the validate-only
// classification pass (write-time name-collision checks §5.1, cheap
// negatives): the same per-class EXISTS probes the resolver chain runs,
// without building any definition. It over-approximates module roots whose
// kind gates need row flags; a read failure logs and classifies as NOT
// served.
func (s *Store) NameExists(ctx context.Context, name string) bool {
	ok, err := s.nameExists(ctx, name)
	if err != nil {
		logReadErr("name exists "+name, err)
		return false
	}
	return ok
}

func (s *Store) nameExists(ctx context.Context, name string) (bool, error) {
	if _, ok := topLevelRoots[name]; ok {
		return true, nil
	}
	// System types and the shared-type stubs live in the static layer.
	if s.static.ForName(ctx, name) != nil {
		return true, nil
	}
	g := s.genCtx()
	if exists, err := g.dataObjectExists(ctx, name); err != nil || exists {
		return exists, err
	}
	if exists, err := g.typeExists(ctx, name); err != nil || exists {
		return exists, err
	}
	for _, ref := range classifyModuleRootName(name) {
		modules, err := g.resolveModulePath(ctx, ref.Module)
		if err != nil {
			return false, err
		}
		if len(modules) > 0 {
			return true, nil
		}
	}
	for i := range derivedRules {
		base, ok := derivedRules[i].match(name)
		if !ok {
			continue
		}
		// Derived types are built over data objects AND structural residual
		// types (buildFilter / buildMutInput* / buildAggregation fallbacks).
		if exists, err := g.dataObjectExists(ctx, base); err != nil || exists {
			return exists, err
		}
		if exists, err := g.typeExists(ctx, base); err != nil || exists {
			return exists, err
		}
	}
	return false, nil
}

// typeExists probes a catalog.types row (activity-gated).
func (g *genContext) typeExists(ctx context.Context, name string) (bool, error) {
	names, err := g.queryNames(ctx, `SELECT t.name FROM core.catalog.types t`+
		activeMeta("m", "t.data_source")+` WHERE t.name = `+lit(name))
	return len(names) > 0, err
}
