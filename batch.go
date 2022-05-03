// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"context"
	"runtime"
	"sync"
	"time"
)

// Options represent parameters for batch.Processor. They should be passed to StartProcessor function. All options
// (as the name suggest) are optional and have default values.
type Options[Resource any] struct {
	// All batches will be run for at least MinDuration.
	//
	// By default, 100ms.
	MinDuration time.Duration
	// Batch will have timeout with MaxDuration. Context with this timeout will be passed to
	// LoadResource and SaveResource functions, which can abort the batch by returning an error.
	//
	// By default, 2*MinDuration.
	MaxDuration time.Duration
	// LoadResource loads resource with given key from a database. Returning an error aborts the batch.
	// This function is called in the beginning of each new batch. Context passed as a first parameter
	// has a timeout calculated using batch MaxDuration. You can use this information to abort loading resource
	// if it takes too long.
	//
	// By default, returns zero-value Resource.
	LoadResource func(_ context.Context, key string) (Resource, error)
	// SaveResource saves resource with given key to a database. Returning an error aborts the batch.
	// This function is called at the end of each batch. Context passed as a first parameter
	// has a timeout calculated using batch MaxDuration. You can use this information to abort saving resource
	// if it takes too long (thus aborting the entire batch).
	//
	// By default, does nothing.
	SaveResource func(_ context.Context, key string, _ Resource) error
	// GoRoutines specifies how many goroutines should be used to run batch operations.
	//
	// By default, 16 * number of CPUs.
	GoRoutines int
	// GoRoutineNumberForKey returns go-routine number which will be used to run operation on
	// a given resource key. This function is crucial to properly serialize requests.
	//
	// This function must be deterministic - it should always return the same go-routine number
	// for given combination of key and goroutines parameters.
	//
	// By default, GoroutineNumberForKey function is used. This implementation calculates hash
	// on a given key and use modulo to calculate go-routine number.
	GoRoutineNumberForKey func(key string, goroutines int) int
}

// StartProcessor starts batch processor which will run operations in batches.
//
// Please note that Processor is a go-routine pool internally and should be stopped when no longer needed.
// Please use Processor.Stop method to stop it.
func StartProcessor[Resource any](options Options[Resource]) *Processor[Resource] {
	options = options.withDefaults()

	workerChannels := make([]chan operation[Resource], options.GoRoutines)

	var workersFinished sync.WaitGroup
	workersFinished.Add(options.GoRoutines)

	for i := 0; i < options.GoRoutines; i++ {
		workerChannels[i] = make(chan operation[Resource])
		_worker := worker[Resource]{
			goRoutineNumber:    i,
			incomingOperations: workerChannels[i],
			loadResource:       options.LoadResource,
			saveResource:       options.SaveResource,
			minDuration:        options.MinDuration,
			maxDuration:        options.MaxDuration,
		}

		go func() {
			_worker.run()
			workersFinished.Done()
		}()
	}

	return &Processor[Resource]{
		options:         options,
		stopped:         make(chan struct{}),
		workerChannels:  workerChannels,
		workersFinished: &workersFinished,
	}
}

// Processor represents instance of batch processor which can be used to issue operations which run in a batch manner.
type Processor[Resource any] struct {
	options         Options[Resource]
	stopped         chan struct{}
	workerChannels  []chan operation[Resource]
	workersFinished *sync.WaitGroup
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

// Run lets you run an operation on a resource with given key. Operation will run along other operations in batches.
// If there is no pending batch then the new batch will be started and will run for at least MinDuration. After the
// MinDuration no new operations will be accepted and SaveResource function will be called.
//
// Operations are run sequentially. No manual locking is required inside operation. Operation should be fast, which
// basically means that any I/O should be avoided at all cost.
//
// Run ends when the entire batch has ended. It returns error when batch is aborted or processor is stopped.
// Only LoadResource and SaveResource functions can abort the batch by returning an error. If error was reported
// for a batch all Run calls assigned to this batch will get this error.
//
// Operation which is still waiting to be run can be canceled by cancelling ctx. If operation was executed but batch
// is pending then Run waits until batch ends. When ctx is cancelled then OperationCancelled error is returned.
func (p *Processor[Resource]) Run(ctx context.Context, key string, _operation func(Resource)) error {
	select {
	case <-p.stopped:
		return ProcessorStopped
	default:
	}

	result := make(chan error)
	defer close(result)

	goRoutineNumber := p.options.GoRoutineNumberForKey(key, p.options.GoRoutines)

	o := operation[Resource]{
		resourceKey: key,
		run:         _operation,
		result:      result,
	}

	select {
	case p.workerChannels[goRoutineNumber] <- o:
		return <-result
	case <-ctx.Done():
		return OperationCancelled
	}
}

// Stop ends all running batches. No new operations will be accepted.
// Stop blocks until all pending batches are ended and resources saved.
func (p *Processor[Resource]) Stop() {
	close(p.stopped)

	for _, channel := range p.workerChannels {
		close(channel)
	}

	p.workersFinished.Wait()
}
