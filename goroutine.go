// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"context"
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
}

func (b *batch[Resource]) process() {
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
				return
			}

			b.results = append(b.results, _operation.result)
			_operation.run(*b.resource)
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
}

func (b *batch[Resource]) save(ctx context.Context) error {
	if err := b.SaveResource(ctx, b.resourceKey, *b.resource); err != nil {
		return err
	}

	return nil
}

func (b *batch[Resource]) load(ctx context.Context) error {
	if b.alreadyLoaded() {
		return nil
	}

	resource, err := b.LoadResource(ctx, b.resourceKey)
	if err != nil {
		return err
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
