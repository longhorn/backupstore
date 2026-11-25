package s3

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
)

// fakeListS3Server is an in-process stand-in for an S3-compatible backend that
// implements just enough of ListObjectsV2 (prefix, delimiter, pagination) to
// compare the request count of the old per-directory walk against the new flat,
// paginated ListRecursive. It needs no bucket, credentials or network access,
// so the comparison runs deterministically in CI.
//
// Regression test for https://github.com/longhorn/longhorn/issues/1547
type fakeListS3Server struct {
	mu       sync.Mutex
	objects  map[string]int64
	pageSize int
}

func newFakeListS3Server() *fakeListS3Server {
	return &fakeListS3Server{objects: map[string]int64{}, pageSize: 1000}
}

// put seeds an object key. Seeding is done through the map directly so it is
// not counted, keeping the measured request counts to the listing itself.
func (f *fakeListS3Server) put(key string) {
	f.mu.Lock()
	f.objects[key] = 1
	f.mu.Unlock()
}

func (f *fakeListS3Server) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if r.Method != http.MethodGet || !q.Has("list-type") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")

	f.mu.Lock()
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	f.mu.Unlock()
	sort.Strings(keys)

	start := 0
	if token := q.Get("continuation-token"); token != "" {
		if _, err := fmt.Sscanf(token, "%d", &start); err != nil {
			start = 0
		}
	}

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
	if start >= len(entries) {
		writeFakeListResult(w, prefix, delimiter, nil, nil, false, "")
		return
	}
	end := start + f.pageSize
	if end > len(entries) {
		end = len(entries)
	}
	page := entries[start:end]

	var pageContents, pagePrefixes []string
	for _, e := range page {
		if delimiter != "" && strings.HasSuffix(e, delimiter) {
			pagePrefixes = append(pagePrefixes, e)
		} else {
			pageContents = append(pageContents, e)
		}
	}

	token := ""
	if end < len(entries) {
		token = fmt.Sprintf("%d", end)
	}
	writeFakeListResult(w, prefix, delimiter, pageContents, pagePrefixes, end < len(entries), token)
}

func writeFakeListResult(w http.ResponseWriter, prefix, delimiter string, contents, prefixes []string, truncated bool, token string) {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>test-bucket</Name><Prefix>`)
	b.WriteString(prefix)
	b.WriteString(`</Prefix><KeyCount>`)
	fmt.Fprintf(&b, "%d", len(contents)+len(prefixes))
	b.WriteString(`</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>`)
	if truncated {
		b.WriteString("true")
	} else {
		b.WriteString("false")
	}
	b.WriteString(`</IsTruncated>`)
	if truncated {
		if token != "" {
			fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", token)
		}
	}
	if delimiter != "" {
		b.WriteString(`<Delimiter>/</Delimiter>`)
	}
	for _, c := range contents {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>1</Size></Contents>", c)
	}
	for _, p := range prefixes {
		fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", p)
	}
	b.WriteString("</ListBucketResult>")

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, b.String())
}

// TestListRecursiveRequestCountVsNestedWalk asserts, over a synthetic block
// tree, that ListRecursive uses a constant number of ListObjectsV2 requests
// (one per 1000 keys) while the old per-directory walk scales with the number
// of shard directories.
func TestListRecursiveRequestCountVsNestedWalk(t *testing.T) {
	fake := newFakeListS3Server()
	server := httptest.NewServer(http.HandlerFunc(fake.handle))
	defer server.Close()

	// 3000 blocks spread across the two-level shard tree, roughly one block per
	// directory - the realistic shape for a volume with many blocks.
	const blocks = 3000
	for i := 0; i < blocks; i++ {
		fake.put(fmt.Sprintf("backupstore/volumes/blocks/%02x/%02x/block-%04d.blk", i%256, (i/256)%256, i))
	}

	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_ENDPOINTS", server.URL)
	t.Setenv("VIRTUAL_HOSTED_STYLE", "false")

	drvOld, countOld := newCountingDriver(t, "s3://test-bucket@us-east-1/backupstore")
	names := nestedWalk(t, drvOld, "volumes/blocks")

	drvNew, countNew := newCountingDriver(t, "s3://test-bucket@us-east-1/backupstore")
	got, err := drvNew.ListRecursive("volumes/blocks")
	if err != nil {
		t.Fatalf("ListRecursive failed: %v", err)
	}

	oldList, new := int(countOld.list), int(countNew.list)
	t.Logf("3000 blocks across %d shard directories: nested walk = %d list requests, ListRecursive = %d list requests",
		shardDirs(blocks), oldList, new)

	if len(names) != blocks || len(got) != blocks {
		t.Fatalf("block count mismatch: nested walk %d, recursive %d, want %d", len(names), len(got), blocks)
	}
	if new >= oldList {
		t.Fatalf("ListRecursive (%d requests) did not beat the nested walk (%d requests)", new, oldList)
	}
	// 3000 keys at 1000 keys per page is 3 requests, independent of tree shape.
	if new > 3 {
		t.Fatalf("ListRecursive used %d list requests, expected at most 3 for 3000 keys", new)
	}
	if oldList < blocks {
		t.Fatalf("nested walk used only %d requests, expected roughly one per block directory", oldList)
	}
}

func shardDirs(blocks int) int {
	seen := map[string]bool{}
	for i := 0; i < blocks; i++ {
		seen[fmt.Sprintf("%02x/%02x", i%256, (i/256)%256)] = true
	}
	return len(seen)
}
