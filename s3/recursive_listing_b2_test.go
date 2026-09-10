package s3

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

type countingRoundTripper struct {
	base http.RoundTripper
	list int64
	all  int64
}

func (c *countingRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	atomic.AddInt64(&c.all, 1)
	if r.Method == http.MethodGet && r.URL.Query().Has("list-type") {
		atomic.AddInt64(&c.list, 1)
	}
	return c.base.RoundTrip(r)
}

func newCountingDriver(t *testing.T, backupURL string) (*BackupStoreDriver, *countingRoundTripper) {
	t.Helper()
	d, err := initFunc(backupURL)
	if err != nil {
		t.Fatalf("initFunc: %v", err)
	}
	drv := d.(*BackupStoreDriver)
	base := drv.service.Client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	crt := &countingRoundTripper{base: base}
	drv.service.Client.Transport = crt
	return drv, crt
}

// nestedWalk reproduces the pre-fix getBlockNamesForVolume walk: one List() per directory level.
func nestedWalk(t *testing.T, driver *BackupStoreDriver, blockPathBase string) []string {
	t.Helper()
	names := []string{}
	lv1Dirs, err := driver.List(blockPathBase)
	if err != nil {
		return names
	}
	for _, lv1 := range lv1Dirs {
		lv1Path := filepath.Join(blockPathBase, lv1)
		lv2Dirs, err := driver.List(lv1Path)
		if err != nil {
			t.Fatalf("List(%s): %v", lv1Path, err)
		}
		for _, lv2 := range lv2Dirs {
			lv2Path := filepath.Join(lv1Path, lv2)
			blocks, err := driver.List(lv2Path)
			if err != nil {
				t.Fatalf("List(%s): %v", lv2Path, err)
			}
			for _, b := range blocks {
				if strings.HasSuffix(b, ".blk") {
					names = append(names, b)
				}
			}
		}
	}
	return names
}

// TestRecursiveVsNestedListingRequestCountAgainstB2 counts real B2 list requests for the old nested walk vs the new recursive listing. Skipped unless LONGHORN_B2_* env is set.
func TestRecursiveVsNestedListingRequestCountAgainstB2(t *testing.T) {
	endpoint := os.Getenv("LONGHORN_B2_ENDPOINT")
	region := os.Getenv("LONGHORN_B2_REGION")
	bucket := os.Getenv("LONGHORN_B2_BUCKET")
	blockBase := os.Getenv("LONGHORN_B2_BLOCK_BASE")
	if endpoint == "" || region == "" || bucket == "" || blockBase == "" {
		t.Skip("LONGHORN_B2_* env not set; skipping real-backend recursive listing test")
	}

	t.Setenv("AWS_ENDPOINTS", endpoint)
	t.Setenv("VIRTUAL_HOSTED_STYLE", "false")

	backupURL := "s3://" + bucket + "@" + region + "/"

	drvNew, cNew := newCountingDriver(t, backupURL)
	recPaths, err := drvNew.ListRecursive(blockBase)
	if err != nil {
		t.Fatalf("ListRecursive: %v", err)
	}

	drvOld, cOld := newCountingDriver(t, backupURL)
	oldNames := nestedWalk(t, drvOld, blockBase)

	t.Logf("block prefix: %s", blockBase)
	t.Logf("OLD nested walk: blocks=%d  S3 list requests=%d (total http=%d)",
		len(oldNames), atomic.LoadInt64(&cOld.list), atomic.LoadInt64(&cOld.all))
	t.Logf("NEW recursive:   blocks=%d  S3 list requests=%d (total http=%d)",
		len(recPaths), atomic.LoadInt64(&cNew.list), atomic.LoadInt64(&cNew.all))

	newList := atomic.LoadInt64(&cNew.list)
	oldList := atomic.LoadInt64(&cOld.list)
	if newList >= oldList {
		t.Fatalf("recursive listing (%d) did not reduce list requests vs nested walk (%d)", newList, oldList)
	}
	if newList > 5 {
		t.Fatalf("recursive listing issued %d list requests; expected a small constant (<=5)", newList)
	}
}
