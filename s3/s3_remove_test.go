package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type fakeObjectDeleteClient struct {
	mu        sync.Mutex
	calls     []string
	active    int
	maxActive int
	delay     time.Duration
	failures  map[string]error
}

func (f *fakeObjectDeleteClient) DeleteObject(ctx context.Context, input *awss3.DeleteObjectInput,
	_ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	key := aws.ToString(input.Key)

	f.mu.Lock()
	f.calls = append(f.calls, key)
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.mu.Unlock()

	if f.delay > 0 {
		select {
		case <-ctx.Done():
		case <-time.After(f.delay):
		}
	}

	f.mu.Lock()
	f.active--
	err := f.failures[key]
	f.mu.Unlock()

	return &awss3.DeleteObjectOutput{}, err
}

func (f *fakeObjectDeleteClient) snapshot() ([]string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := append([]string(nil), f.calls...)
	return calls, f.maxActive
}

func TestRemoveFilesUsesBoundedConcurrency(t *testing.T) {
	const concurrency = 4
	paths := make([]string, 32)
	for i := range paths {
		paths[i] = fmt.Sprintf("blocks/%02d.blk", i)
	}
	client := &fakeObjectDeleteClient{delay: 10 * time.Millisecond}

	err := removeFiles(context.Background(), client, "bucket", paths,
		func(path string) string { return "prefix/" + path }, concurrency)
	if err != nil {
		t.Fatalf("removeFiles failed: %v", err)
	}

	calls, maxActive := client.snapshot()
	if maxActive != concurrency {
		t.Fatalf("expected concurrency to reach %d, got %d", concurrency, maxActive)
	}
	if len(calls) != len(paths) {
		t.Fatalf("expected %d deletes, got %d", len(paths), len(calls))
	}
	sort.Strings(calls)
	for i, path := range paths {
		expected := "prefix/" + path
		if calls[i] != expected {
			t.Fatalf("expected call %q, got %q", expected, calls[i])
		}
	}
}

func TestRemoveFilesAttemptsEveryPathAndAggregatesErrors(t *testing.T) {
	paths := []string{"a.blk", "b.blk", "c.blk", "d.blk"}
	client := &fakeObjectDeleteClient{
		failures: map[string]error{
			"a.blk": errors.New("first failure"),
			"c.blk": errors.New("second failure"),
		},
	}

	err := removeFiles(context.Background(), client, "bucket", paths, func(path string) string { return path }, 2)
	if err == nil {
		t.Fatal("expected removeFiles to return an aggregate error")
	}
	if !strings.Contains(err.Error(), "failed to delete 2 exact objects") ||
		!strings.Contains(err.Error(), "a.blk") || !strings.Contains(err.Error(), "c.blk") {
		t.Fatalf("unexpected aggregate error: %v", err)
	}

	calls, maxActive := client.snapshot()
	if len(calls) != len(paths) {
		t.Fatalf("expected every path to be attempted, got %v", calls)
	}
	if maxActive > 2 {
		t.Fatalf("expected at most 2 concurrent deletes, got %d", maxActive)
	}
}

func TestRemoveFilesBoundsFailureDetails(t *testing.T) {
	paths := make([]string, maxRemovalFailureSamples+5)
	failures := make(map[string]error, len(paths))
	for i := range paths {
		paths[i] = fmt.Sprintf("%02d.blk", i)
		failures[paths[i]] = errors.New("delete failed")
	}
	client := &fakeObjectDeleteClient{failures: failures}

	err := removeFiles(context.Background(), client, "bucket", paths, func(path string) string { return path }, 3)
	if err == nil {
		t.Fatal("expected removeFiles to return an aggregate error")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("failed to delete %d exact objects", len(paths))) {
		t.Fatalf("unexpected aggregate error: %v", err)
	}
	if details := strings.Count(err.Error(), "delete failed"); details != maxRemovalFailureSamples {
		t.Fatalf("expected %d sampled failure details, got %d: %v", maxRemovalFailureSamples, details, err)
	}
}

func TestRemoveFilesRejectsInvalidConcurrency(t *testing.T) {
	client := &fakeObjectDeleteClient{}
	err := removeFiles(context.Background(), client, "bucket", []string{"block.blk"},
		func(path string) string { return path }, 0)
	if err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("expected an invalid concurrency error, got %v", err)
	}
	if calls, _ := client.snapshot(); len(calls) != 0 {
		t.Fatalf("expected no delete attempts, got %v", calls)
	}
}

func TestDeleteObjectTreatsMissingKeyAsSuccess(t *testing.T) {
	client := &fakeObjectDeleteClient{
		failures: map[string]error{
			"missing.blk": &smithy.GenericAPIError{Code: "NoSuchKey", Message: "not found"},
		},
	}

	for i := 0; i < 2; i++ {
		if err := deleteObject(context.Background(), client, "bucket", "missing.blk"); err != nil {
			t.Fatalf("expected a repeated missing-key delete to be idempotent, got %v", err)
		}
	}
}

func TestDeleteObjectDoesNotIgnoreMissingBucket(t *testing.T) {
	client := &fakeObjectDeleteClient{
		failures: map[string]error{
			"block.blk": &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "not found"},
		},
	}

	err := deleteObject(context.Background(), client, "bucket", "block.blk")
	if err == nil || !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("expected the missing bucket error to be preserved, got %v", err)
	}
}

func TestRemoveFilesIssuesOnlyExactDeleteRequests(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []recordedRequest
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		requests = append(requests, recordedRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery})
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	driver := &BackupStoreDriver{
		path:    "backup-root",
		service: newTestService(t, server.URL),
	}
	paths := []string{
		"blocks/aa/bb/first.blk",
		"blocks/aa/bb/second.blk",
		"blocks/cc/dd/third.blk",
	}

	if err := driver.RemoveFiles(paths); err != nil {
		t.Fatalf("RemoveFiles failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != len(paths) {
		t.Fatalf("expected exactly %d requests, got %d: %+v", len(paths), len(requests), requests)
	}
	expected := map[string]bool{}
	for _, path := range paths {
		expected["/test-bucket/backup-root/"+path] = true
	}
	for _, request := range requests {
		if request.method != http.MethodDelete {
			t.Fatalf("expected only DELETE requests, got %+v", request)
		}
		if request.query != "x-id=DeleteObject" {
			t.Fatalf("expected only the DeleteObject operation query, got %+v", request)
		}
		if !expected[request.path] {
			t.Fatalf("unexpected object path %q", request.path)
		}
		delete(expected, request.path)
	}
	if len(expected) != 0 {
		t.Fatalf("missing exact delete requests: %v", expected)
	}
}

type delayedObjectDeleteClient struct {
	delay time.Duration
}

func (d delayedObjectDeleteClient) DeleteObject(ctx context.Context, _ *awss3.DeleteObjectInput,
	_ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(d.delay):
		return &awss3.DeleteObjectOutput{}, nil
	}
}

func BenchmarkRemoveFiles(b *testing.B) {
	paths := make([]string, 100)
	for i := range paths {
		paths[i] = fmt.Sprintf("blocks/%03d.blk", i)
	}
	client := delayedObjectDeleteClient{delay: time.Millisecond}

	for _, concurrency := range []int{1, maxConcurrentExactFileRemovals} {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := removeFiles(context.Background(), client, "bucket", paths,
					func(path string) string { return path }, concurrency); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
