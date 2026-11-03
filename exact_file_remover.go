package backupstore

import (
	"iter"

	"github.com/cockroachdb/errors"
)

const (
	exactFileRemovalInitialCapacity   = 1024
	exactFileRemovalBatchSize         = 64 * 1024
	maxExactFileRemovalFailureSamples = 10
)

// ExactFileRemover is an optional extension of BackupStoreDriver for removing
// a set of known files without first expanding each path as a prefix.
//
// Implementations must remove only the supplied paths, attempt every removal
// before returning an aggregate error, bound any internal concurrency, and not
// retain paths after returning. RemoveFiles does not imply use of a provider's
// multi-object delete API.
type ExactFileRemover interface {
	RemoveFiles(paths []string) error
}

// RemoveExactFiles feeds paths to remover in bounded batches. It continues
// after a batch error so every path is attempted, then returns a bounded error
// summary. The iterator avoids materializing every GC candidate at once.
func RemoveExactFiles(remover ExactFileRemover, paths iter.Seq[string]) error {
	batch := make([]string, 0, exactFileRemovalInitialCapacity)
	failureCount := 0
	failureSamples := make([]error, 0, maxExactFileRemovalFailureSamples)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := remover.RemoveFiles(batch); err != nil {
			failureCount++
			if len(failureSamples) < maxExactFileRemovalFailureSamples {
				failureSamples = append(failureSamples, err)
			}
		}
		batch = batch[:0]
	}

	for path := range paths {
		batch = append(batch, path)
		if len(batch) == exactFileRemovalBatchSize {
			flush()
		}
	}
	flush()

	if failureCount > 0 {
		return errors.Wrapf(errors.Join(failureSamples...), "failed to remove exact files in %d batches", failureCount)
	}
	return nil
}
