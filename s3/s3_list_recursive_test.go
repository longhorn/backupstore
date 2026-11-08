package s3

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const fakeListPageSize = 1000

// fakeListServer serves ListObjectsV2 from a fixed key set in pages of fakeListPageSize keys, and records
// the query of every request.
type fakeListServer struct {
	*httptest.Server
	keys []string

	mu      sync.Mutex
	queries []string
}

func newFakeListServer(keys []string) *fakeListServer {
	sort.Strings(keys)
	f := &fakeListServer{keys: keys}
	f.Server = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeListServer) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f.mu.Lock()
	f.queries = append(f.queries, r.URL.RawQuery)
	f.mu.Unlock()

	if r.Method != http.MethodGet || q.Get("list-type") != "2" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if q.Get("delimiter") != "" {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	var matched []string
	for _, k := range f.keys {
		if strings.HasPrefix(k, q.Get("prefix")) {
			matched = append(matched, k)
		}
	}
	start, _ := strconv.Atoi(q.Get("continuation-token"))
	end := min(start+fakeListPageSize, len(matched))
	truncated := end < len(matched)

	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	fmt.Fprintf(&b, `<Name>test-bucket</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>`,
		q.Get("prefix"), end-start, fakeListPageSize, truncated)
	for _, k := range matched[start:end] {
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>1</Size></Contents>`, k)
	}
	if truncated {
		fmt.Fprintf(&b, `<NextContinuationToken>%d</NextContinuationToken>`, end)
	}
	b.WriteString(`</ListBucketResult>`)

	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(b.String()))
}

func (f *fakeListServer) recordedQueries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

func TestListRecursiveSendsOneRequestPerPage(t *testing.T) {
	const blockCount = 2500
	blocksDir := "root/backupstore/volumes/7e/ed/pvc-1/blocks/"

	keys := []string{
		blocksDir + "0a/", // a directory marker
		"root/backupstore/volumes/7e/ed/pvc-1/volume.cfg", // outside the listed prefix
	}
	var want []string
	for i := range blockCount {
		rel := fmt.Sprintf("%02x/%02x/%064x.blk", i%256, i/256, i)
		keys = append(keys, blocksDir+rel)
		want = append(want, rel)
	}

	server := newFakeListServer(keys)
	defer server.Close()

	driver := &BackupStoreDriver{path: "root", service: newTestService(t, server.URL)}

	got, err := driver.ListRecursive("backupstore/volumes/7e/ed/pvc-1/blocks/")
	if err != nil {
		t.Fatalf("ListRecursive failed: %v", err)
	}

	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("expected %d files, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("file %d: expected %q, got %q", i, want[i], got[i])
		}
	}

	queries := server.recordedQueries()
	if wantRequests := (blockCount + 1 + fakeListPageSize - 1) / fakeListPageSize; len(queries) != wantRequests {
		t.Fatalf("expected %d ListObjectsV2 requests, got %d: %v", wantRequests, len(queries), queries)
	}
}
