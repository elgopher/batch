// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"context"
	"time"
)

type worker[Resource any] struct {
	goRoutineNumber    int
	incomingOperations <-chan operation[Resource]
	loadResource       func(context.Context, string) (Resource, error)
	saveResource       func(context.Context, string, Resource) error
	minDuration        time.Duration
	maxDuration        time.Duration

	batchByResourceKey map[string]*batch[Resource]
	batchByDeadline    []*batch[Resource]
}

func (w *worker[Resource]) run() {
	w.batchByResourceKey = map[string]*batch[Resource]{}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.endBatchesAfterDeadline()
		case _operation, ok := <-w.incomingOperations:
			if !ok {
				w.endAllBatches()
				return
			}

			w.runOperation(_operation)
		}
	}
}

func (w *worker[Resource]) endBatchesAfterDeadline() {
	now := time.Now()

	for _, _batch := range w.batchByDeadline {
		if _batch.deadline.After(now) {
			return
		}

		err := w.saveResource(_batch.ctx, _batch.key, _batch.resource)
		_batch.publishResult(err)
		delete(w.batchByResourceKey, _batch.key)
		w.batchByDeadline = w.batchByDeadline[1:]
	}
}

func (w *worker[Resource]) endAllBatches() {
	for key, _batch := range w.batchByResourceKey {
		err := w.saveResource(_batch.ctx, key, _batch.resource)
		_batch.publishResult(err)
	}

	w.batchByResourceKey = map[string]*batch[Resource]{}
	w.batchByDeadline = nil
}

func (w *worker[Resource]) runOperation(_operation operation[Resource]) {
	_batch, found := w.batchByResourceKey[_operation.resourceKey]
	if !found {
		ctx, _ := context.WithTimeout(context.Background(), w.maxDuration)

		now := time.Now()

		resource, err := w.loadResource(ctx, _operation.resourceKey)
		if err != nil {
			_operation.result <- err
			return
		}

		_batch = &batch[Resource]{
			ctx:      ctx,
			key:      _operation.resourceKey,
			resource: resource,
			deadline: now.Add(w.minDuration),
		}
		w.batchByResourceKey[_operation.resourceKey] = _batch
		w.batchByDeadline = append(w.batchByDeadline, _batch)
	}

	_batch.results = append(_batch.results, _operation.result)

	_operation.run(_batch.resource)
}

type operation[Resource any] struct {
	resourceKey string
	run         func(Resource)
	result      chan error
}

type batch[Resource any] struct {
	ctx      context.Context
	key      string
	resource Resource
	results  []chan error
	deadline time.Time
}

func (b *batch[Resource]) publishResult(result error) {
	for _, c := range b.results {
		c <- result
	}
}
