package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// IngestResult is the success payload returned by /ipc/ingest.
type IngestResult struct {
	DataObject string   `json:"data_object"`
	Inserted   int64    `json:"inserted"`
	Columns    []string `json:"columns"`
}

const ingestContentType = "application/vnd.apache.arrow.stream"

// arrowFileMagic identifies the Arrow IPC *file* format (random-access),
// distinct from the IPC *stream* format that /ipc/ingest expects on the wire.
var arrowFileMagic = []byte("ARROW1")

// IngestStream POSTs the given Arrow IPC stream to /ipc/ingest. The body
// must already be a valid Arrow IPC stream (schema message followed by
// record batches) — typically produced by ipc.NewWriter, by another tool,
// or read from a stream-format file.
//
// The body is forwarded to the server without intermediate buffering. Use
// this when the caller already has a serialised stream from disk, the
// network, or another process. Use Ingest for the higher-level API that
// serialises an array.RecordReader for you.
func (c *Client) IngestStream(ctx context.Context, dataObject string, body io.Reader) (*IngestResult, error) {
	if dataObject == "" {
		return nil, errors.New("hugr ingest: data_object is required")
	}
	if body == nil {
		return nil, errors.New("hugr ingest: body is nil")
	}
	endpoint, err := buildIngestURL(c.url, dataObject)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", ingestContentType)
	setAsUserHeaders(ctx, req)
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		var ebody struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &ebody)
		if ebody.Error == "" {
			ebody.Error = strings.TrimSpace(string(raw))
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

// Ingest streams the records produced by reader into the target data object.
// Columns from the Arrow schema must match insertable fields of the table
// (computed/virtual/reference fields are rejected by the server).
//
// dataObject is either a dotted GraphQL Query path (e.g. "pg_store.public.events")
// or a bare hugr type name. The client serialises the reader as an Apache
// Arrow IPC stream and POSTs it to /ipc/ingest on the configured base URL.
//
// The reader is fully drained on success; on error the caller may inspect
// the reader's remaining state but it should be released by the caller in
// all cases.
func (c *Client) Ingest(ctx context.Context, dataObject string, reader array.RecordReader) (*IngestResult, error) {
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

	res, httpErr := c.IngestStream(ctx, dataObject, pr)
	if httpErr != nil {
		// Unblock the writer goroutine if the HTTP side aborted early.
		_ = pr.CloseWithError(httpErr)
	}
	if werr := <-writeErr; werr != nil {
		// Serialisation errors are more informative than the (likely
		// derivative) HTTP error.
		return nil, werr
	}
	return res, httpErr
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

// IngestArrowIPCFile opens an Arrow IPC file at path and streams its
// contents to /ipc/ingest. Both IPC formats are accepted:
//
//   - stream format (no ARROW1 prefix) — written by ipc.NewWriter or
//     pyarrow.ipc.new_stream. Bytes are forwarded directly to the server,
//     zero-copy.
//   - file format (.arrow / .feather, starts with ARROW1 magic) — written
//     by ipc.NewFileWriter or pyarrow.feather.write_feather. The file is
//     read sequentially via ipc.FileReader and re-emitted as a stream.
func (c *Client) IngestArrowIPCFile(ctx context.Context, dataObject, path string) (*IngestResult, error) {
	if path == "" {
		return nil, errors.New("hugr ingest: path is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	// Peek for the ARROW1 magic to decide between stream and file format.
	var magic [6]byte
	n, err := io.ReadFull(f, magic[:])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if n == len(arrowFileMagic) && bytes.Equal(magic[:], arrowFileMagic) {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek %s: %w", path, err)
		}
		fr, err := ipc.NewFileReader(f, ipc.WithAllocator(memory.NewGoAllocator()))
		if err != nil {
			return nil, fmt.Errorf("open arrow ipc file %s: %w", path, err)
		}
		defer fr.Close()
		rr := &fileReaderAsRecordReader{fr: fr}
		rr.refCount.Add(1)
		defer rr.Release()
		return c.Ingest(ctx, dataObject, rr)
	}

	// Stream format — forward bytes. Prepend the bytes we already consumed
	// during magic detection.
	body := io.MultiReader(bytes.NewReader(magic[:n]), f)
	return c.IngestStream(ctx, dataObject, body)
}

// NewLazyReader returns an array.RecordReader that produces batches by
// calling gen. gen should return (batch, nil) for each successive batch and
// (nil, nil) to signal end-of-stream. Returning (_, err) terminates the
// reader; the error is then visible via Err().
//
// The reader takes ownership of each returned batch and releases it on the
// next Next() call or on the final Release. The caller must not Release
// the batch themselves after returning it from gen.
//
// Typical use: stream bulk data from any source (file, channel, generator)
// into Client.Ingest without implementing the full array.RecordReader
// interface by hand.
func NewLazyReader(schema *arrow.Schema, gen func() (arrow.RecordBatch, error)) array.RecordReader {
	r := &lazyReader{schema: schema, gen: gen}
	r.refCount.Add(1)
	return r
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

// --- lazyReader -----------------------------------------------------------

type lazyReader struct {
	schema *arrow.Schema
	gen    func() (arrow.RecordBatch, error)

	cur      arrow.RecordBatch
	err      error
	done     bool
	refCount atomic.Int64
}

func (r *lazyReader) Schema() *arrow.Schema { return r.schema }
func (r *lazyReader) Err() error            { return r.err }

func (r *lazyReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.done || r.err != nil {
		return false
	}
	rec, err := r.gen()
	if err != nil {
		r.err = err
		r.done = true
		return false
	}
	if rec == nil {
		r.done = true
		return false
	}
	r.cur = rec
	return true
}

func (r *lazyReader) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *lazyReader) Record() arrow.RecordBatch      { return r.cur }
func (r *lazyReader) Retain()                        { r.refCount.Add(1) }
func (r *lazyReader) Release() {
	if r.refCount.Add(-1) == 0 {
		if r.cur != nil {
			r.cur.Release()
			r.cur = nil
		}
	}
}

// --- fileReaderAsRecordReader ---------------------------------------------

// fileReaderAsRecordReader adapts an *ipc.FileReader (random-access file
// format) to the array.RecordReader interface required by Ingest.
type fileReaderAsRecordReader struct {
	fr  *ipc.FileReader
	cur arrow.RecordBatch
	err error

	refCount atomic.Int64
}

func (r *fileReaderAsRecordReader) Schema() *arrow.Schema { return r.fr.Schema() }
func (r *fileReaderAsRecordReader) Err() error            { return r.err }

func (r *fileReaderAsRecordReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.err != nil {
		return false
	}
	rec, err := r.fr.Read()
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		r.err = err
		return false
	}
	if rec == nil {
		return false
	}
	// FileReader.Read documents that the record is valid until next Read.
	// Retain so we own the reference until our own Next/Release.
	rec.Retain()
	r.cur = rec
	return true
}

func (r *fileReaderAsRecordReader) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *fileReaderAsRecordReader) Record() arrow.RecordBatch      { return r.cur }
func (r *fileReaderAsRecordReader) Retain()                        { r.refCount.Add(1) }
func (r *fileReaderAsRecordReader) Release() {
	if r.refCount.Add(-1) == 0 {
		if r.cur != nil {
			r.cur.Release()
			r.cur = nil
		}
	}
}
