package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
