// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"context"
	"time"
)

type operation[Resource any] struct {
	resourceKey string
	run         func(Resource)
	result      chan error
}

type worker[Resource any] struct {
	goRoutineNumber    int
	incomingOperations <-chan operation[Resource]
	loadResource       func(context.Context, string) (Resource, error)
	saveResource       func(context.Context, string, Resource) error
	minDuration        time.Duration
	maxDuration        time.Duration
}

func (w worker[Resource]) run() {
	batchByResourceKey := map[string]*batch[Resource]{}
	var batchByDeadline []*batch[Resource]

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			for _, _batch := range batchByDeadline {
				if _batch.deadline.Before(now) {
					err := w.saveResource(_batch.ctx, _batch.key, _batch.resource)
					_batch.publishResult(err)
					delete(batchByResourceKey, _batch.key)
					batchByDeadline = batchByDeadline[1:]
					continue
				}
			}
		case _operation, ok := <-w.incomingOperations:
			if !ok {
				for key, _batch := range batchByResourceKey {
					err := w.saveResource(_batch.ctx, key, _batch.resource)
					_batch.publishResult(err)
					continue
				}
				return
			}

			_batch, found := batchByResourceKey[_operation.resourceKey]
			if !found {
				ctx, cancel := context.WithTimeout(context.Background(), w.maxDuration)
				defer cancel()

				now := time.Now()

				resource, err := w.loadResource(ctx, _operation.resourceKey)
				if err != nil {
					_operation.result <- err
					continue
				}
				_batch = &batch[Resource]{
					ctx:      ctx,
					key:      _operation.resourceKey,
					resource: resource,
					deadline: now.Add(w.minDuration),
				}
				batchByResourceKey[_operation.resourceKey] = _batch
				batchByDeadline = append(batchByDeadline, _batch)
			}

			_batch.results = append(_batch.results, _operation.result)

			_operation.run(_batch.resource)
		}
	}
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
