// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch_test

import (
	"testing"

	"github.com/elgopher/batch"
	"github.com/stretchr/testify/assert"
)

func TestGoroutineNumberForKey(t *testing.T) {
	t.Run("should return number from 0 to goroutines-1", func(t *testing.T) {
		goroutines := 8
		keys := []string{"key", "another", "", " ", "_", "KEY", "!"}
		for _, key := range keys {
			n := batch.GoroutineNumberForKey(key, goroutines)
			assert.GreaterOrEqual(t, n, 0)
			assert.Less(t, n, goroutines)
		}
	})
}
