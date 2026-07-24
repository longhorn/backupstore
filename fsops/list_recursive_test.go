package fsops

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

type testOps struct {
	root string
}

func (t *testOps) LocalPath(path string) string {
	return filepath.Join(t.root, path)
}

func TestFileSystemOperatorListRecursive(t *testing.T) {
	root, err := os.MkdirTemp("", "fsops-recursive-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	ops := &testOps{root: root}
	f := NewFileSystemOperator(ops)

	paths := []string{
		filepath.Join("volumes", "aa", "bb", "pvc1", "blocks", "11", "22", "abc.blk"),
		filepath.Join("volumes", "aa", "bb", "pvc1", "blocks", "11", "33", "def.blk"),
	}
	for _, p := range paths {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	result, err := f.ListRecursive("volumes")
	if err != nil {
		t.Fatalf("ListRecursive failed: %v", err)
	}
	sort.Strings(result)
	expected := []string{
		filepath.Join("aa", "bb", "pvc1", "blocks", "11", "22", "abc.blk"),
		filepath.Join("aa", "bb", "pvc1", "blocks", "11", "33", "def.blk"),
	}
	sort.Strings(expected)
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i := range expected {
		if result[i] != expected[i] {
			t.Fatalf("expected %v, got %v", expected, result)
		}
	}
}

func TestFileSystemOperatorListRecursiveMissingDir(t *testing.T) {
	root, err := os.MkdirTemp("", "fsops-recursive-missing-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	ops := &testOps{root: root}
	f := NewFileSystemOperator(ops)

	result, err := f.ListRecursive("does-not-exist")
	if err != nil {
		t.Fatalf("expected no error for missing dir, got: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty result, got: %v", result)
	}
}
