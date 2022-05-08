// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"context"
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
	// This function is called in the beginning of each new batch.
	//
	// Context passed as a first parameter has a timeout calculated using batch MaxDuration.
	// You can watch context cancellation in order to abort loading resource if it takes too long.
	// Context is also cancelled after batch was ended.
	//
	// By default, returns zero-value Resource.
	LoadResource func(_ context.Context, key string) (Resource, error)
	// SaveResource saves resource with given key to a database. Returning an error aborts the batch.
	// This function is called at the end of each batch.
	//
	// Context passed as a first parameter has a timeout calculated using batch MaxDuration.
	// You can watch context cancellation in order to abort saving resource if it takes too long
	// (thus aborting the entire batch). Context is also cancelled after batch was ended.
	//
	// By default, does nothing.
	SaveResource func(_ context.Context, key string, _ Resource) error
}

// StartProcessor starts batch processor which will run operations in batches.
//
// Please note that Processor is a go-routine pool internally and should be stopped when no longer needed.
// Please use Processor.Stop method to stop it.
func StartProcessor[Resource any](options Options[Resource]) *Processor[Resource] {
	options = options.withDefaults()

	return &Processor[Resource]{
		options:       options,
		stopped:       make(chan struct{}),
		batchChannels: map[string]chan operation[Resource]{},
	}
}

// Processor represents instance of batch processor which can be used to issue operations which run in a batch manner.
type Processor[Resource any] struct {
	options            Options[Resource]
	stopped            chan struct{}
	allBatchesFinished sync.WaitGroup
	mutex              sync.Mutex
	batchChannels      map[string]chan operation[Resource]
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

	return s
}

// Run lets you run an operation on a resource with given key. Operation will run along other operations in batches.
// If there is no pending batch then the new batch will be started and will run for at least MinDuration. After the
// MinDuration no new operations will be accepted and SaveResource function will be called.
//
// Operations are run sequentially. No manual synchronization is required inside operation. Operation should be fast, which
// basically means that any I/O should be avoided at all cost. Operations (together with LoadResource and SaveResource)
// are run on a batch dedicated go-routine.
//
// Operation must leave Resource in a consistent state, so the next operation in batch can be executed on the same resource.
// When operation cannot be executed because some conditions are not met then operation should not change the state
// of resource at all. This could be achieved easily by dividing operation into two sections:
//
//  - first section validates if operation is possible and returns error if not
//  - second section change the Resource state
//
// Run ends when the entire batch has ended.
//
// Error is returned when batch is aborted or processor is stopped. Only LoadResource and SaveResource functions can abort
// the batch by returning an error. If error was reported for a batch, all Run calls assigned to this batch will get this error.
//
// Please always check the returned error. Operations which query the resource get uncommitted data. If there is
// a problem with saving changes to the database, then you could have a serious inconsistency between your db and what you've
// just sent to the users.
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

	operationMessage := operation[Resource]{
		run:    _operation,
		result: result,
	}

	for {
		incomingOperations := p.temporaryBatchChannel(key)

		select {
		case <-ctx.Done():
			return OperationCancelled

		case incomingOperations <- operationMessage:
			return <-result

		case <-time.After(10 * time.Millisecond):
			// Timeout waiting to push operation. Possibly batch goroutine was stopped.
		}
	}

}

func (p *Processor[Resource]) temporaryBatchChannel(key string) chan<- operation[Resource] {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	batchChannel, ok := p.batchChannels[key]
	if !ok {
		batchChannel = make(chan operation[Resource])
		p.batchChannels[key] = batchChannel

		go p.startBatch(key, batchChannel)
	}

	return batchChannel
}

func (p *Processor[Resource]) startBatch(key string, batchChannel chan operation[Resource]) {
	p.allBatchesFinished.Add(1)
	defer p.allBatchesFinished.Done()

	now := time.Now()

	w := &batch[Resource]{
		Options:            p.options,
		resourceKey:        key,
		incomingOperations: batchChannel,
		stopped:            p.stopped,
		softDeadline:       now.Add(p.options.MinDuration),
		hardDeadline:       now.Add(p.options.MaxDuration),
	}
	w.process()

	p.mutex.Lock()
	defer p.mutex.Unlock()
	// Delete the channel even though it is still used by pending Run calls.
	// Those calls should time out and retry on a new channel.
	delete(p.batchChannels, key)
}

// Stop ends all running batches. No new operations will be accepted.
// Stop blocks until all pending batches are ended and resources saved.
func (p *Processor[Resource]) Stop() {
	close(p.stopped)

	p.allBatchesFinished.Wait()
}
