package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// recordedRequest captures just enough information about an incoming request
// to assert which S3 operation (single PutObject vs multipart upload) was
// actually sent over the wire.
type recordedRequest struct {
	method string
	path   string
	query  string
}

// fakeS3Server fakes just enough of the S3 API (PutObject,
// CreateMultipartUpload, UploadPart, CompleteMultipartUpload) to let the AWS
// SDK complete an upload, while recording every request it receives. This
// lets tests assert on the request shape without needing a real S3-compatible
// backend or credentials.
type fakeS3Server struct {
	*httptest.Server

	mu       sync.Mutex
	requests []recordedRequest
}

func newFakeS3Server() *fakeS3Server {
	f := &fakeS3Server{}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeS3Server) handle(w http.ResponseWriter, r *http.Request) {
	// Drain the request body fully before responding. Otherwise the
	// connection may be closed with unread data still in flight, which some
	// HTTP clients (including the AWS SDK's transport) treat as a retryable
	// network error, causing the same logical request to be retried and
	// throwing off the request counts asserted below.
	_, _ = io.Copy(io.Discard, r.Body)

	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		method: r.Method,
		path:   r.URL.Path,
		query:  r.URL.RawQuery,
	})
	f.mu.Unlock()

	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		// CreateMultipartUpload
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-key</Key><UploadId>test-upload-id</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPut && q.Has("partNumber"):
		w.Header().Set("ETag", `"part-etag"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		// CompleteMultipartUpload
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>test-key</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == http.MethodPut:
		w.Header().Set("ETag", `"put-etag"`)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (f *fakeS3Server) recordedRequests() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedRequest, len(f.requests))
	copy(out, f.requests)
	return out
}

// newTestService returns a service pointed at the given fake endpoint,
// without relying on real AWS credentials or a live S3-compatible backend.
func newTestService(t *testing.T, endpoint string) *service {
	t.Helper()

	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_ENDPOINTS", endpoint)

	return &service{
		Region: "us-east-1",
		Bucket: "test-bucket",
	}
}

// TestWriteUsesSinglePutObjectForLargePayload is a regression test for large
// backup_*.cfg files (whose size grows with the number of blocks in a
// backup) being silently routed through a multipart upload once they cross
// the AWS SDK's default 5 MiB PartSize threshold. Some S3-interop providers
// (e.g. Google Cloud Storage) reject that multipart request shape with
// SignatureDoesNotMatch, even though a plain PutObject request of the same
// object succeeds. It asserts that PutObjectAsSinglePart (used by
// BackupStoreDriver.Write, i.e. metadata and data blocks) always issues a
// single PutObject request, even for a payload well above that threshold.
func TestWriteUsesSinglePutObjectForLargePayload(t *testing.T) {
	server := newFakeS3Server()
	defer server.Close()

	svc := newTestService(t, server.URL)

	// 8 MiB payload: bigger than the SDK's default 5 MiB multipart threshold.
	payload := bytes.Repeat([]byte("a"), 8*1024*1024)

	if err := svc.PutObjectAsSinglePart(context.Background(), "backups/backup_test.cfg", bytes.NewReader(payload)); err != nil {
		t.Fatalf("PutObjectAsSinglePart failed: %v", err)
	}

	requests := server.recordedRequests()
	if len(requests) != 1 {
		t.Fatalf("expected exactly 1 request, got %d: %+v", len(requests), requests)
	}

	req := requests[0]
	if req.method != http.MethodPut {
		t.Fatalf("expected a single PUT request, got %s", req.method)
	}
	if strings.Contains(req.query, "uploads") || strings.Contains(req.query, "partNumber") || strings.Contains(req.query, "uploadId") {
		t.Fatalf("expected no multipart query parameters, got query %q", req.query)
	}
}

// TestPutObjectUsesMultipartForLargePayload documents the contrasting
// behavior of the multipart-capable PutObject path (used by Upload, e.g.
// single-file backups and system-backup bundles): a payload above the
// default 5 MiB threshold does go through
// CreateMultipartUpload/UploadPart/CompleteMultipartUpload.
func TestPutObjectUsesMultipartForLargePayload(t *testing.T) {
	server := newFakeS3Server()
	defer server.Close()

	svc := newTestService(t, server.URL)

	payload := bytes.Repeat([]byte("a"), 8*1024*1024)

	if err := svc.PutObject(context.Background(), "backups/large-file.bak", bytes.NewReader(payload)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	var sawCreateMultipart, sawUploadPart, sawComplete bool
	for _, req := range server.recordedRequests() {
		switch {
		case req.method == http.MethodPost && strings.Contains(req.query, "uploads"):
			sawCreateMultipart = true
		case req.method == http.MethodPut && strings.Contains(req.query, "partNumber"):
			sawUploadPart = true
		case req.method == http.MethodPost && strings.Contains(req.query, "uploadId"):
			sawComplete = true
		}
	}

	if !sawCreateMultipart || !sawUploadPart || !sawComplete {
		t.Fatalf("expected a multipart upload sequence, got requests: %+v", server.recordedRequests())
	}
}

// TestPutObjectAsSinglePartRejectsOversizedPayload verifies that
// PutObjectAsSinglePart fails fast with a clear error, instead of sending a
// request that a single PutObject cannot fulfill, when the payload exceeds
// the 5 GiB single PutObject limit.
func TestPutObjectAsSinglePartRejectsOversizedPayload(t *testing.T) {
	server := newFakeS3Server()
	defer server.Close()

	svc := newTestService(t, server.URL)

	oversized := &fakeSizedReadSeeker{size: maxSinglePutObjectSize + 1}

	err := svc.PutObjectAsSinglePart(context.Background(), "backups/backup_test.cfg", oversized)
	if err == nil {
		t.Fatal("expected an error for an oversized payload, got nil")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected a clear size-limit error, got: %v", err)
	}

	if requests := server.recordedRequests(); len(requests) != 0 {
		t.Fatalf("expected no requests to be sent for an oversized payload, got: %+v", requests)
	}
}

// fakeSizedReadSeeker is an io.ReadSeeker stub that reports an arbitrary
// size via Seek(0, io.SeekEnd) without actually allocating that much memory,
// used to exercise the size-limit check without allocating multiple
// gigabytes in a unit test.
type fakeSizedReadSeeker struct {
	size int64
	pos  int64
}

func (f *fakeSizedReadSeeker) Read(p []byte) (int, error) {
	if f.pos >= f.size {
		return 0, fmt.Errorf("EOF")
	}
	n := int64(len(p))
	if remaining := f.size - f.pos; n > remaining {
		n = remaining
	}
	f.pos += n
	return int(n), nil
}

func (f *fakeSizedReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0: // io.SeekStart
		f.pos = offset
	case 1: // io.SeekCurrent
		f.pos += offset
	case 2: // io.SeekEnd
		f.pos = f.size + offset
	}
	return f.pos, nil
}
