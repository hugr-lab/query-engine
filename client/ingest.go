package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// IngestResult is the success payload returned by /ipc/ingest.
type IngestResult struct {
	DataObject string   `json:"data_object"`
	Inserted   int64    `json:"inserted"`
	Columns    []string `json:"columns"`
}

// Ingest streams the records produced by reader into the target data object.
// Columns from the Arrow schema must match insertable fields of the table
// (computed/virtual/reference fields are rejected by the server).
//
// dataObject is either a dotted GraphQL Query path (e.g. "pg_store.public.events")
// or a bare hugr type name. The client serializes the reader as an Apache Arrow
// IPC stream and POSTs it to /ipc/ingest on the configured base URL.
//
// The reader is fully drained on success; on error the caller may inspect the
// reader's remaining state but it should be released by the caller in all cases.
func (c *Client) Ingest(ctx context.Context, dataObject string, reader array.RecordReader) (*IngestResult, error) {
	if dataObject == "" {
		return nil, errors.New("hugr ingest: data_object is required")
	}
	if reader == nil {
		return nil, errors.New("hugr ingest: reader is nil")
	}

	pr, pw := io.Pipe()
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeErr)
		iw := ipc.NewWriter(pw, ipc.WithSchema(reader.Schema()))
		var streamErr error
		for reader.Next() {
			rec := reader.RecordBatch()
			if rec == nil {
				continue
			}
			if err := iw.Write(rec); err != nil {
				streamErr = fmt.Errorf("write arrow record: %w", err)
				break
			}
		}
		if streamErr == nil {
			if err := reader.Err(); err != nil {
				streamErr = fmt.Errorf("read arrow record: %w", err)
			}
		}
		if err := iw.Close(); err != nil && streamErr == nil {
			streamErr = fmt.Errorf("close arrow writer: %w", err)
		}
		_ = pw.CloseWithError(streamErr)
		writeErr <- streamErr
	}()

	endpoint, err := buildIngestURL(c.url, dataObject)
	if err != nil {
		_ = pr.Close()
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, pr)
	if err != nil {
		_ = pr.Close()
		return nil, err
	}
	req.Header.Set("Content-Type", "application/vnd.apache.arrow.stream")
	setAsUserHeaders(ctx, req)

	resp, err := c.c.Do(req)
	if err != nil {
		_ = pr.CloseWithError(err)
		return nil, err
	}
	defer resp.Body.Close()

	// Surface a writer-side error in preference to the (likely derivative)
	// HTTP error.
	if werr := <-writeErr; werr != nil {
		return nil, werr
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		var ebody struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &ebody)
		if ebody.Error == "" {
			ebody.Error = strings.TrimSpace(string(body))
		}
		if ebody.Error == "" {
			ebody.Error = resp.Status
		}
		return nil, fmt.Errorf("hugr ingest: %s: %s", resp.Status, ebody.Error)
	}

	var out IngestResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode ingest response: %w", err)
	}
	return &out, nil
}

// IngestRecord is a single-batch convenience wrapper around Ingest. It builds
// an array.RecordReader from a single arrow.RecordBatch and forwards.
func (c *Client) IngestRecord(ctx context.Context, dataObject string, rec arrow.RecordBatch) (*IngestResult, error) {
	if rec == nil {
		return nil, errors.New("hugr ingest: record is nil")
	}
	rr, err := array.NewRecordReader(rec.Schema(), []arrow.RecordBatch{rec})
	if err != nil {
		return nil, fmt.Errorf("build record reader: %w", err)
	}
	defer rr.Release()
	return c.Ingest(ctx, dataObject, rr)
}

// buildIngestURL derives the /ipc/ingest endpoint from the client's base /ipc URL.
// Accepts both ".../ipc" (canonical) and ".../ipc/" forms.
func buildIngestURL(base, dataObject string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("invalid hugr url %q: %w", base, err)
	}
	path := strings.TrimSuffix(u.Path, "/")
	switch {
	case strings.HasSuffix(path, "/ipc"):
		u.Path = path + "/ingest"
	case strings.HasSuffix(path, "/ipc/ingest"):
		// already pointed at ingest endpoint — keep as-is
	default:
		u.Path = path + "/ipc/ingest"
	}
	q := u.Query()
	q.Set("data_object", dataObject)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
