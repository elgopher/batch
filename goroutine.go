// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"context"
	"fmt"
	"time"
)

type batch[Resource any] struct {
	Options[Resource]
	resourceKey        string
	incomingOperations <-chan operation[Resource]
	stopped            <-chan struct{} // stopped is used to stop batch prematurely
	softDeadline       time.Time
	hardDeadline       time.Time

	resource *Resource
	results  []chan error

	metric Metric
}

func (b *batch[Resource]) process() {
	b.metric.ResourceKey = b.resourceKey
	b.metric.BatchStart = time.Now()
	defer func() {
		b.metric.TotalDuration = time.Since(b.metric.BatchStart)
	}()

	hardDeadlineContext, cancel := context.WithDeadline(context.Background(), b.hardDeadline)
	defer cancel()

	softDeadlineReached := time.NewTimer(b.softDeadline.Sub(time.Now()))
	defer softDeadlineReached.Stop()

	for {
		select {
		case <-b.stopped:
			b.end(hardDeadlineContext)
			return

		case <-softDeadlineReached.C:
			b.end(hardDeadlineContext)
			return

		case _operation := <-b.incomingOperations:
			err := b.load(hardDeadlineContext)
			if err != nil {
				_operation.result <- err
				b.metric.Error = err
				return
			}

			b.results = append(b.results, _operation.result)
			_operation.run(*b.resource)
			b.metric.OperationCount++
		}
	}
}

func (b *batch[Resource]) end(ctx context.Context) {
	if b.resource == nil {
		return
	}

	err := b.save(ctx)
	for _, result := range b.results {
		result <- err
	}
	b.metric.Error = err
}

func (b *batch[Resource]) save(ctx context.Context) error {
	started := time.Now()
	defer func() {
		b.metric.SaveResourceDuration = time.Since(started)
	}()

	if err := b.SaveResource(ctx, b.resourceKey, *b.resource); err != nil {
		return fmt.Errorf("saving resource failed: %w", err)
	}

	return nil
}

func (b *batch[Resource]) load(ctx context.Context) error {
	if b.alreadyLoaded() {
		return nil
	}

	started := time.Now()
	defer func() {
		b.metric.LoadResourceDuration = time.Since(started)
	}()

	resource, err := b.LoadResource(ctx, b.resourceKey)
	if err != nil {
		return fmt.Errorf("loading resource failed: %w", err)
	}

	b.resource = &resource

	return nil
}

func (b *batch[Resource]) alreadyLoaded() bool {
	return b.resource != nil
}

type operation[Resource any] struct {
	run    func(Resource)
	result chan error
}
