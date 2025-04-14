package hugr

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hugr-lab/query-engine/pkg/cache"
	"github.com/hugr-lab/query-engine/pkg/jq"
)

type JQRequest struct {
	JQ    string  `json:"jq"`
	Query Request `json:"query"`
}

// jqHandler execute query and apply jq transformation to the result
func (s *Service) jqHandler(w http.ResponseWriter, r *http.Request) {
	// handle http request
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dataFunc := func() (any, error) {
		var req JQRequest
		err := json.Unmarshal(b, &req)
		if err != nil {
			return nil, err
		}
		var t *jq.Transformer
		if req.JQ != "" {
			t, err = jq.NewTransformer(req.JQ, map[string]any{"$vars": req.Query.Variables})
			if err != nil {
				return nil, fmt.Errorf("JQ compiler: %w", err)
			}
		}

		res := s.ProcessQuery(r.Context(), "", req.Query)

		if res.Err() != nil {
			return nil, res.Err()
		}

		if req.JQ == "" {
			return json.Marshal(res)
		}
		// handle jq request
		transformed, err := t.Transform(r.Context(), res, map[string]any{"$vars": req.Query.Variables})
		if err != nil {
			return nil, fmt.Errorf("JQ transform: %w", err)
		}
		return json.Marshal(transformed)
	}

	w.Header().Add("Content-Type", "application/json")
	cached := r.Header.Get("X-Hugr-Cache")
	if cached == "" {
		data, err := dataFunc()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_, err = w.Write(data.([]byte))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	ttl, _ := time.ParseDuration(cached)
	info := cache.Info{
		Use:        true,
		Key:        r.Header.Get("X-Hugr-Cache-Key"),
		TTL:        time.Duration(ttl) * time.Second,
		Tags:       strings.Split(r.Header.Get("X-Hugr-Cache-Tags"), ","),
		Invalidate: r.Header.Get("X-Hugr-Cache-Invalidate") == "true",
	}
	if info.Key == "" {
		info.Key, err = cache.QueryKey(string(b), nil)
	}
	data, err := s.cache.Load(r.Context(), info.Key, dataFunc, info.Options()...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if info.Invalidate {
		err = s.cache.Invalidate(r.Context(), info.Tags...)
		if err != nil {
			http.Error(w, "cache invalidation: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	_, err = w.Write(data.([]byte))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
