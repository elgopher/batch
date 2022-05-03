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

		w.endBatch(_batch)

		delete(w.batchByResourceKey, _batch.resourceKey)
		w.batchByDeadline = w.batchByDeadline[1:]
	}
}

func (w *worker[Resource]) endBatch(_batch *batch[Resource]) {
	ctx, cancel := context.WithDeadline(context.Background(), _batch.deadline)
	err := w.saveResource(ctx, _batch.resourceKey, _batch.resource)
	_batch.publishResult(err)
	cancel()
}

func (w *worker[Resource]) endAllBatches() {
	for _, _batch := range w.batchByDeadline {
		w.endBatch(_batch)
	}

	w.batchByResourceKey = map[string]*batch[Resource]{}
	w.batchByDeadline = nil
}

func (w *worker[Resource]) runOperation(_operation operation[Resource]) {
	_batch, found := w.batchByResourceKey[_operation.resourceKey]
	if !found {
		var err error
		_batch, err = w.newBatch(_operation.resourceKey)
		if err != nil {
			_operation.result <- err
			return
		}

		w.addBatch(_batch)
	}

	_batch.operationResults = append(_batch.operationResults, _operation.result)

	_operation.run(_batch.resource)
}

func (w *worker[Resource]) newBatch(resourceKey string) (*batch[Resource], error) {
	now := time.Now()
	deadline := now.Add(w.minDuration)

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	resource, err := w.loadResource(ctx, resourceKey)
	if err != nil {
		return nil, err
	}

	return &batch[Resource]{
		resourceKey: resourceKey,
		resource:    resource,
		deadline:    deadline,
	}, nil
}

func (w *worker[Resource]) addBatch(b *batch[Resource]) {
	w.batchByResourceKey[b.resourceKey] = b
	w.batchByDeadline = append(w.batchByDeadline, b)
}

type operation[Resource any] struct {
	resourceKey string
	run         func(Resource)
	result      chan error
}

type batch[Resource any] struct {
	resourceKey      string
	resource         Resource
	operationResults []chan error
	deadline         time.Time
}

func (b *batch[Resource]) publishResult(result error) {
	for _, r := range b.operationResults {
		r <- result
	}
}
