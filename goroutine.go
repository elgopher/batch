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
	softDeadlineReached := time.NewTimer(b.softDeadline.Sub(time.Now()))
	defer softDeadlineReached.Stop()

	for {
		select {
		case <-b.stopped:
			b.end()
			return

		case <-softDeadlineReached.C:
			b.end()
			return

		case _operation := <-b.incomingOperations:
			err := b.load()
			if err != nil {
				_operation.result <- err
				return
			}

			b.results = append(b.results, _operation.result)
			_operation.run(*b.resource)
		}
	}
}

func (b *batch[Resource]) end() {
	if b.resource == nil {
		return
	}

	err := b.save()
	for _, result := range b.results {
		result <- err
	}
}

func (b *batch[Resource]) save() error {
	ctx, cancel := context.WithDeadline(context.Background(), b.hardDeadline)
	defer cancel()

	if err := b.SaveResource(ctx, b.resourceKey, *b.resource); err != nil {
		return err
	}

	return nil
}

func (b *batch[Resource]) load() error {
	if b.alreadyLoaded() {
		return nil
	}

	ctx, cancel := context.WithDeadline(context.Background(), b.hardDeadline)
	defer cancel()

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
