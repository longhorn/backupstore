package s3

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// liveRequestCounts categorises the requests a run issued, so the per-block
// delete path can be compared with the batched path against a real
// S3-compatible backend.
type liveRequestCounts struct {
	list        int64
	deleteOne   int64
	deleteBatch int64
	other       int64
}

func (c *liveRequestCounts) classify(r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Has("list-type"):
		atomic.AddInt64(&c.list, 1)
	case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
		atomic.AddInt64(&c.deleteBatch, 1)
	case r.Method == http.MethodDelete:
		atomic.AddInt64(&c.deleteOne, 1)
	default:
		atomic.AddInt64(&c.other, 1)
	}
}

// TestBatchDeleteVsPerBlockAgainstLiveS3 measures backup GC delete traffic
// against a real S3-compatible backend (MinIO, moto, Backblaze B2, ...).
//
// Skipped unless LONGHORN_S3_ENDPOINT, LONGHORN_S3_REGION, LONGHORN_S3_BUCKET
// and LONGHORN_S3_BLOCK_BASE are set, so CI never needs a bucket:
//
//	LONGHORN_S3_ENDPOINT=http://localhost:5000 LONGHORN_S3_REGION=us-east-1 \
//	LONGHORN_S3_BUCKET=test LONGHORN_S3_BLOCK_BASE=gc-test go test -run LiveS3 ./s3/
//
// https://github.com/longhorn/longhorn/issues/14510
func TestBatchDeleteVsPerBlockAgainstLiveS3(t *testing.T) {
	endpoint := os.Getenv("LONGHORN_S3_ENDPOINT")
	region := os.Getenv("LONGHORN_S3_REGION")
	bucket := os.Getenv("LONGHORN_S3_BUCKET")
	blockBase := os.Getenv("LONGHORN_S3_BLOCK_BASE")
	if endpoint == "" || region == "" || bucket == "" || blockBase == "" {
		t.Skip("LONGHORN_S3_* env not set; skipping live S3 batch delete test")
	}

	t.Setenv("AWS_ENDPOINTS", endpoint)
	t.Setenv("VIRTUAL_HOSTED_STYLE", "false")

	d, err := initFunc("s3://" + bucket + "@" + region + "/" + blockBase + "/")
	if err != nil {
		t.Fatalf("initFunc: %v", err)
	}
	drv := d.(*BackupStoreDriver)

	const blocks = 200
	paths := make([]string, 0, blocks)
	for i := 0; i < blocks; i++ {
		path := fmt.Sprintf("%s/%02x/%02x/block-%04d.blk", strings.TrimSuffix(blockBase, "/"), i%256, (i/256)%256, i)
		// Relative to the driver path, which is what cleanupBlocks passes.
		rel := strings.TrimPrefix(path, blockBase+"/")
		paths = append(paths, rel)
	}

	// Seed without counting the seeding traffic.
	for _, p := range paths {
		if err := drv.Write(p, strings.NewReader("x")); err != nil {
			t.Fatalf("seed %v: %v", p, err)
		}
	}

	// Old path: a ListObjects of the prefix plus a DeleteObject per block.
	before := &liveRequestCounts{}
	drv.service.Client.Transport = &countingTransport{base: http.DefaultTransport, counts: before}
	for _, p := range paths {
		if err := drv.service.DeleteObjects(context.Background(), drv.updatePath(p)); err != nil {
			t.Fatalf("old path %v: %v", p, err)
		}
	}

	// New path: known keys are deleted in batches of 1000.
	after := &liveRequestCounts{}
	drv.service.Client.Transport = &countingTransport{base: http.DefaultTransport, counts: after}
	for _, p := range paths {
		if err := drv.Write(p, strings.NewReader("x")); err != nil {
			t.Fatalf("reseed %v: %v", p, err)
		}
	}
	if err := drv.RemoveBatch(paths); err != nil {
		t.Fatalf("batch path: %v", err)
	}

	t.Logf("%d blocks: old = %d list + %d delete requests, new = %d list + %d batch delete requests",
		blocks, before.list, before.deleteOne+before.deleteBatch, after.list, after.deleteOne+after.deleteBatch)

	remaining, err := drv.List(strings.TrimSuffix(blockBase, "/") + "/")
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("batch delete left %d blocks behind on the live backend", len(remaining))
	}

	if before.list != blocks || before.deleteOne+before.deleteBatch != blocks {
		t.Fatalf("old path baseline unexpected: list=%d delete=%d", before.list, before.deleteOne+before.deleteBatch)
	}
	if after.list != 0 || after.deleteOne+after.deleteBatch > 1 {
		t.Fatalf("batch path expected 0 list and 1 request, got list=%d delete=%d", after.list, after.deleteOne+after.deleteBatch)
	}
}

type countingTransport struct {
	base   http.RoundTripper
	counts *liveRequestCounts
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.counts.classify(r)
	return c.base.RoundTrip(r)
}
