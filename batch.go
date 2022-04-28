// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"context"
	"runtime"
	"time"
)

type Options[Resource any] struct {
	MinDuration           time.Duration
	MaxDuration           time.Duration
	LoadResource          func(_ context.Context, key string) (Resource, error)
	SaveResource          func(_ context.Context, key string, _ Resource) error
	GoRoutines            int
	GoRoutineNumberForKey func(_ string, goroutines int) int
}

func StartProcessor[Resource any](options Options[Resource]) *Processor[Resource] {
	options = options.withDefaults()

	operations := make([]chan operation[Resource], options.GoRoutines)
	for i := 0; i < options.GoRoutines; i++ {
		operations[i] = make(chan operation[Resource])
		w := worker[Resource]{
			goRoutineNumber: i,
			operations:      operations[i],
			loadResource:    options.LoadResource,
			saveResource:    options.SaveResource,
			minDuration:     options.MinDuration,
			maxDuration:     options.MaxDuration,
		}
		go w.run()
	}

	return &Processor[Resource]{
		options:    options,
		stopped:    make(chan struct{}),
		operations: operations,
	}
}

type Processor[Resource any] struct {
	options    Options[Resource]
	stopped    chan struct{}
	operations []chan operation[Resource]
}

func (s Options[Resource]) withDefaults() Options[Resource] {
	if s.LoadResource == nil {
		s.LoadResource = func(context.Context, string) (Resource, error) {
			var r Resource
			return r, nil
		}
	}

	if s.SaveResource == nil {
		s.SaveResource = func(context.Context, string, Resource) error {
			return nil
		}
	}

	if s.MinDuration == 0 {
		s.MinDuration = 100 * time.Millisecond
	}

	if s.MaxDuration == 0 {
		s.MaxDuration = 2 * s.MinDuration
	}

	if s.GoRoutines == 0 {
		s.GoRoutines = 16 * runtime.NumCPU()
	}

	if s.GoRoutineNumberForKey == nil {
		s.GoRoutineNumberForKey = GoroutineNumberForKey
	}

	return s
}

// Run lets you run an operation which will be run along other operations in a single batch (as a single atomic transaction).
// If there is no pending batch then the batch will be started. Operations are run sequentially.
//
// Run ends when the entire batch has ended.
func (p *Processor[Resource]) Run(key string, op func(Resource)) error {
	select {
	case <-p.stopped:
		return ProcessorStopped
	default:
	}

	result := make(chan error)
	defer close(result)

	goRoutineNumber := p.options.GoRoutineNumberForKey(key, p.options.GoRoutines)

	p.operations[goRoutineNumber] <- operation[Resource]{
		resourceKey: key,
		run:         op,
		result:      result,
	}

	return <-result
}

// Stop ends all running batches. No new operations will be accepted.
func (p *Processor[Resource]) Stop() {
	close(p.stopped)
	for _, op := range p.operations {
		close(op)
	}
}
