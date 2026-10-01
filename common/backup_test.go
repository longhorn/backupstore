package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// receiveWithTimeout returns the next value from ch, or fails the test if none arrives in time.
func receiveWithTimeout(t *testing.T, ch <-chan error) (err error, ok bool) {
	t.Helper()
	select {
	case err, ok = <-ch:
		return err, ok
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting on the merged error channel")
		return nil, false // unreachable, t.Fatal stops the test
	}
}

func TestMergeErrorChannelsRelaysWorkerError(t *testing.T) {
	assert := assert.New(t)

	workerErr := errors.New("worker failed")
	failed := make(chan error, 1)
	failed <- workerErr
	running := make(chan error, 1)

	merged := MergeErrorChannels(context.Background(), failed, running)

	err, ok := receiveWithTimeout(t, merged)
	assert.True(ok)
	assert.ErrorIs(err, workerErr)

	// The remaining worker finishes cleanly; the merged channel must then close.
	close(running)
	_, ok = receiveWithTimeout(t, merged)
	assert.False(ok, "merged channel should close after every input is accounted for")
}

func TestMergeErrorChannelsReportsContextCancellation(t *testing.T) {
	assert := assert.New(t)

	// The merger may observe ctx.Done() before the worker's own error arrives. It must send the
	// cancellation to the output rather than just return, because once every goroutine returns
	// the output is closed and the caller reads nil, which it would report as a successful backup
	// or restore. The backing image backupMappings worker exits on ctx.Done() without sending at
	// all, so this is the only signal its caller gets.
	running := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())
	merged := MergeErrorChannels(ctx, running)
	cancel()

	err, ok := receiveWithTimeout(t, merged)
	assert.True(ok)
	assert.ErrorIs(err, context.Canceled)

	_, ok = receiveWithTimeout(t, merged)
	assert.False(ok, "merged channel should close after every input is accounted for")
}

func TestMergeErrorChannelsReportsCancellationWhenInputAlreadyClosed(t *testing.T) {
	assert := assert.New(t)

	// When ctx is cancelled and an input is already closed, both select cases are ready and
	// either may win. A worker that exits on ctx.Done() without sending, such as backupMappings,
	// leaves only a closed channel behind, so the merger must still report the cancellation.
	closed := make(chan error)
	close(closed)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	merged := MergeErrorChannels(ctx, closed)

	err, ok := receiveWithTimeout(t, merged)
	assert.True(ok)
	assert.ErrorIs(err, context.Canceled)
}

func TestMergeErrorChannelsClosesWhenAllInputsClose(t *testing.T) {
	assert := assert.New(t)

	first := make(chan error, 1)
	second := make(chan error, 1)
	close(first)
	close(second)

	merged := MergeErrorChannels(context.Background(), first, second)

	err, ok := receiveWithTimeout(t, merged)
	assert.False(ok, "merged channel should close without a value")
	assert.NoError(err)
}

func TestGetProgress(t *testing.T) {
	assert := assert.New(t)

	// Per-block progress must never decrease and must end exactly at ProgressPercentageBackup. The
	// engines treat ProgressPercentageBackupTotal as the terminal state, so only the final status
	// update may report it.
	for _, totalBlocks := range []int64{1, 2, 16, 19, 20, 1000} {
		assert.Equal(0, GetProgress(totalBlocks, 0), "totalBlocks=%d", totalBlocks)
		assert.Equal(ProgressPercentageBackup, GetProgress(totalBlocks, totalBlocks), "totalBlocks=%d", totalBlocks)

		previous := 0
		for processedBlocks := int64(1); processedBlocks <= totalBlocks; processedBlocks++ {
			current := GetProgress(totalBlocks, processedBlocks)
			assert.GreaterOrEqual(current, previous, "totalBlocks=%d processedBlocks=%d", totalBlocks, processedBlocks)
			assert.LessOrEqual(current, ProgressPercentageBackup, "totalBlocks=%d processedBlocks=%d", totalBlocks, processedBlocks)
			previous = current
		}
	}
}
