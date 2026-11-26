package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/longhorn/backupstore"
)

// fakeBatchS3Server is an in-process S3 stand-in that implements enough of
// ListObjectsV2, DeleteObject and DeleteObjects (the batch API) to compare the
// request count of deleting a set of already-known keys the old way - one
// ListObjects plus one DeleteObject per key - against batching them with one
// request per 1000 keys. No bucket, credentials or network access needed.
//
// Regression test for https://github.com/longhorn/longhorn/issues/14510
type fakeBatchS3Server struct {
	mu       sync.Mutex
	objects  map[string]bool
	pageSize int

	listReqs        int
	deleteOneReqs   int
	deleteBatchReqs int
}

func newFakeBatchS3Server() *fakeBatchS3Server {
	return &fakeBatchS3Server{objects: map[string]bool{}, pageSize: 1000}
}

func (f *fakeBatchS3Server) seed(keys []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		f.objects[key] = true
	}
}

// reset clears the counters. initFunc does a test List() against the backend,
// so the driver has to be created before measuring a phase.
func (f *fakeBatchS3Server) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listReqs, f.deleteOneReqs, f.deleteBatchReqs = 0, 0, 0
}

func (f *fakeBatchS3Server) counts() (list, deleteOne, deleteBatch int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listReqs, f.deleteOneReqs, f.deleteBatchReqs
}

func (f *fakeBatchS3Server) remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.objects)
}

func (f *fakeBatchS3Server) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	switch {
	case r.Method == http.MethodGet && q.Has("list-type"):
		f.handleList(w, q)
	case r.Method == http.MethodPost && q.Has("delete"):
		f.handleBatchDelete(w, r)
	case r.Method == http.MethodDelete:
		key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
		f.mu.Lock()
		f.deleteOneReqs++
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (f *fakeBatchS3Server) handleList(w http.ResponseWriter, q url.Values) {
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")

	f.mu.Lock()
	f.listReqs++
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	pageSize := f.pageSize
	f.mu.Unlock()

	sort.Strings(keys)

	var contents, prefixes []string
	if delimiter == "" {
		contents = keys
	} else {
		seen := map[string]bool{}
		for _, k := range keys {
			rest := strings.TrimPrefix(k, prefix)
			if i := strings.Index(rest, delimiter); i >= 0 {
				p := prefix + rest[:i+len(delimiter)]
				if !seen[p] {
					seen[p] = true
					prefixes = append(prefixes, p)
				}
			} else {
				contents = append(contents, k)
			}
		}
	}

	entries := append(append([]string{}, contents...), prefixes...)
	start := 0
	if token := q.Get("continuation-token"); token != "" {
		if _, err := fmt.Sscanf(token, "%d", &start); err != nil {
			start = 0
		}
	}
	end := start + pageSize
	if end > len(entries) {
		end = len(entries)
	}
	truncated := end < len(entries)

	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>`,
		prefix, end-start, pageSize, truncated)
	if delimiter != "" {
		b.WriteString("<Delimiter>/</Delimiter>")
	}
	if truncated {
		fmt.Fprintf(&b, "<NextContinuationToken>%d</NextContinuationToken>", end)
	}
	for _, e := range entries[start:end] {
		if delimiter != "" && strings.HasSuffix(e, delimiter) {
			fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", e)
		} else {
			fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>1</Size></Contents>", e)
		}
	}
	b.WriteString("</ListBucketResult>")

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, b.String())
}

type fakeBatchDeleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

func (f *fakeBatchS3Server) handleBatchDelete(w http.ResponseWriter, r *http.Request) {
	var req fakeBatchDeleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.deleteBatchReqs++
	var deleted, failed []string
	for _, o := range req.Objects {
		if !f.objects[o.Key] {
			failed = append(failed, o.Key)
			continue
		}
		delete(f.objects, o.Key)
		deleted = append(deleted, o.Key)
	}
	f.mu.Unlock()

	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	for _, key := range deleted {
		fmt.Fprintf(&b, "<Deleted><Key>%s</Key></Deleted>", key)
	}
	for _, key := range failed {
		fmt.Fprintf(&b, "<Error><Key>%s</Key><Code>NoSuchKey</Code></Error>", key)
	}
	b.WriteString("</DeleteResult>")

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, b.String())
}

// blockKeys returns count block file paths as backupstore would build them:
// two levels of shards derived from the checksum, one block per directory for a
// volume with a large number of blocks.
func blockKeys(count int) []string {
	keys := make([]string, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, fmt.Sprintf("backupstore/volumes/blocks/%02x/%02x/block-%04d.blk", i%256, (i/256)%256, i))
	}
	return keys
}

func newFakeDriver(t *testing.T, server *httptest.Server) *BackupStoreDriver {
	t.Helper()

	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_ENDPOINTS", server.URL)
	t.Setenv("VIRTUAL_HOSTED_STYLE", "false")

	d, err := initFunc("s3://test-bucket@us-east-1/backupstore")
	if err != nil {
		t.Fatalf("initFunc: %v", err)
	}
	return d.(*BackupStoreDriver)
}

// TestDeleteKnownKeyDoesNotListFirst compares removing one block the caller
// already has the key for: the old path listed the prefix first (2 requests),
// the new path deletes it directly (1 request).
func TestDeleteKnownKeyDoesNotListFirst(t *testing.T) {
	fake := newFakeBatchS3Server()
	server := httptest.NewServer(http.HandlerFunc(fake.handle))
	defer server.Close()

	drv := newFakeDriver(t, server)

	key := "backupstore/volumes/blocks/11/22/pvc-abc/blocks/aa/bb/abcdef.blk"

	fake.seed([]string{key})
	fake.reset()
	if err := drv.service.DeleteObjects(context.Background(), key); err != nil {
		t.Fatalf("old path: %v", err)
	}
	oldList, oldDelete, oldBatch := fake.counts()

	fake.seed([]string{key})
	fake.reset()
	if err := drv.Remove("volumes/blocks/11/22/pvc-abc/blocks/aa/bb/abcdef.blk"); err != nil {
		t.Fatalf("new path: %v", err)
	}
	newList, newDelete, newBatch := fake.counts()

	t.Logf("single known key: old = %d list + %d delete requests, new = %d list + %d delete requests",
		oldList, oldDelete+oldBatch, newList, newDelete+newBatch)

	if oldList != 1 || oldDelete+oldBatch != 1 {
		t.Fatalf("baseline is not the expected 1 list + 1 delete, got list=%d delete=%d", oldList, oldDelete+oldBatch)
	}
	if newList != 0 || newDelete+newBatch != 1 {
		t.Fatalf("expected exactly 1 request and no listing, got list=%d delete=%d", newList, newDelete+newBatch)
	}
	if fake.remaining() != 0 {
		t.Fatalf("block was not deleted, %d objects left", fake.remaining())
	}
}

// TestRemoveBatchUsesOneRequestPer1000Keys compares deleting 2500 known blocks
// one at a time (5000 requests: a ListObjects and a DeleteObject each) with
// batching them into 3 DeleteObjects requests.
func TestRemoveBatchUsesOneRequestPer1000Keys(t *testing.T) {
	fake := newFakeBatchS3Server()
	server := httptest.NewServer(http.HandlerFunc(fake.handle))
	defer server.Close()

	drv := newFakeDriver(t, server)
	if _, ok := interface{}(drv).(backupstore.BatchRemover); !ok {
		t.Fatalf("the s3 driver must implement backupstore.BatchRemover")
	}

	keys := blockKeys(2500)

	fake.seed(keys)
	fake.reset()
	for _, key := range keys {
		if err := drv.service.DeleteObjects(context.Background(), key); err != nil {
			t.Fatalf("old path: %v", err)
		}
	}
	oldList, oldOne, oldBatch := fake.counts()
	if fake.remaining() != 0 {
		t.Fatalf("baseline left %d blocks behind", fake.remaining())
	}

	fake.seed(keys)
	fake.reset()
	paths := make([]string, 0, len(keys))
	for _, key := range keys {
		// The fake stores full object keys; the driver API takes backupstore
		// relative paths.
		paths = append(paths, strings.TrimPrefix(key, "backupstore/"))
	}
	if err := drv.RemoveBatch(paths); err != nil {
		t.Fatalf("batch path: %v", err)
	}
	newList, newOne, newBatch := fake.counts()

	t.Logf("2500 known blocks: old = %d list + %d delete requests, new = %d list + %d delete requests",
		oldList, oldOne+oldBatch, newList, newOne+newBatch)

	if fake.remaining() != 0 {
		t.Fatalf("batch delete left %d blocks behind", fake.remaining())
	}
	if oldList != 2500 || oldOne+oldBatch != 2500 {
		t.Fatalf("baseline is not the expected 2 requests per block, got list=%d delete=%d", oldList, oldOne+oldBatch)
	}
	if newList != 0 || newOne != 0 || newBatch != 3 {
		t.Fatalf("batch delete used %d list and %d delete requests, expected 0 list and 3 batch requests for 2500 keys",
			newList, newOne+newBatch)
	}
}

// TestS3DriverImplementsBatchRemover locks the optional interface in place: the
// s3 driver batches, and callers must still fall back to Remove() for drivers
// that do not (local filesystem, NFS, CIFS), where batching buys nothing.
func TestS3DriverImplementsBatchRemover(t *testing.T) {
	var _ backupstore.BatchRemover = (*BackupStoreDriver)(nil)
}
