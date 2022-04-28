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
	goRoutineNumber int
	operations      chan operation[Resource]
	loadResource    func(context.Context, string) (Resource, error)
	saveResource    func(context.Context, string, Resource) error
	minDuration     time.Duration
	maxDuration     time.Duration
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
			for _, b := range batchByDeadline {
				if b.deadline.Before(now) {
					err := w.saveResource(b.ctx, b.key, b.resource)
					b.publishResult(err)
					delete(batchByResourceKey, b.key)
					batchByDeadline = batchByDeadline[1:]
					continue
				}
			}
		case op, ok := <-w.operations:
			if !ok {
				for key, b := range batchByResourceKey {
					err := w.saveResource(b.ctx, key, b.resource)
					b.publishResult(err)
					continue
				}
				return
			}

			b, found := batchByResourceKey[op.resourceKey]
			if !found {
				ctx, cancel := context.WithTimeout(context.Background(), w.maxDuration)
				defer cancel()

				now := time.Now()

				resource, err := w.loadResource(ctx, op.resourceKey)
				if err != nil {
					op.result <- err
					continue
				}
				b = &batch[Resource]{
					ctx:      ctx,
					key:      op.resourceKey,
					resource: resource,
					deadline: now.Add(w.minDuration),
				}
				batchByResourceKey[op.resourceKey] = b
				batchByDeadline = append(batchByDeadline, b)
			}

			b.results = append(b.results, op.result)

			op.run(b.resource)
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
