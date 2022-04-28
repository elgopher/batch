// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func FutureValue[V any]() Value[V] {
	return Value[V]{
		done: make(chan V, 1),
	}
}

type Value[V any] struct {
	done chan V
}

func (d Value[V]) Set(result V) {
	d.done <- result
}

func (d Value[V]) Get(t *testing.T) V {
	select {
	case r, _ := <-d.done:
		return r
	case <-time.After(time.Second):
		assert.FailNow(t, "timeout waiting for value")
		var r V
		return r
	}
}
