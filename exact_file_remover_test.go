package backupstore

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type batchingTestRemover struct {
	batchSizes []int
	failCalls  map[int]error
}

func (r *batchingTestRemover) RemoveFiles(paths []string) error {
	call := len(r.batchSizes)
	r.batchSizes = append(r.batchSizes, len(paths))
	return r.failCalls[call]
}

func TestRemoveExactFilesBoundsBatchesAndAttemptsAllPaths(t *testing.T) {
	firstErr := errors.New("first batch failed")
	remover := &batchingTestRemover{failCalls: map[int]error{0: firstErr}}
	total := exactFileRemovalBatchSize + 3

	err := RemoveExactFiles(remover, func(yield func(string) bool) {
		for i := 0; i < total; i++ {
			if !yield("block.blk") {
				return
			}
		}
	})
	if err == nil || !strings.Contains(err.Error(), firstErr.Error()) {
		t.Fatalf("expected the first batch error, got %v", err)
	}
	if len(remover.batchSizes) != 2 || remover.batchSizes[0] != exactFileRemovalBatchSize || remover.batchSizes[1] != 3 {
		t.Fatalf("unexpected batches: %v", remover.batchSizes)
	}
}

func TestRemoveExactFilesBoundsFailureDetails(t *testing.T) {
	batchCount := maxExactFileRemovalFailureSamples + 2
	failures := make(map[int]error, batchCount)
	for i := 0; i < batchCount; i++ {
		failures[i] = fmt.Errorf("batch-%d failed", i)
	}
	remover := &batchingTestRemover{failCalls: failures}

	err := RemoveExactFiles(remover, func(yield func(string) bool) {
		for i := 0; i < batchCount*exactFileRemovalBatchSize; i++ {
			if !yield("block.blk") {
				return
			}
		}
	})
	if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("in %d batches", batchCount)) {
		t.Fatalf("unexpected aggregate error: %v", err)
	}
	if !strings.Contains(err.Error(), "batch-9 failed") || strings.Contains(err.Error(), "batch-10 failed") {
		t.Fatalf("expected exactly the first %d failure details, got %v", maxExactFileRemovalFailureSamples, err)
	}
}

func TestRemoveExactFilesSkipsEmptyInput(t *testing.T) {
	remover := &batchingTestRemover{}
	if err := RemoveExactFiles(remover, func(func(string) bool) {}); err != nil {
		t.Fatal(err)
	}
	if len(remover.batchSizes) != 0 {
		t.Fatalf("expected no removal calls, got %v", remover.batchSizes)
	}
}
