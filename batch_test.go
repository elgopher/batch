// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elgopher/batch"
)

func TestProcessor_Run(t *testing.T) {
	t.Run("should run callback on zero-value resource when LoadResource was not provided", func(t *testing.T) {
		futureValue := FutureValue[*resource]()

		processor := batch.StartProcessor(batch.Options[*resource]{})
		defer processor.Stop()
		// when
		err := processor.Run("key", func(c *resource) {
			futureValue.Set(c)
		})
		require.NoError(t, err)

		assert.Nil(t, futureValue.Get(t)) // nil is a zero-value for pointer
	})

	t.Run("should run callback on the loaded resource", func(t *testing.T) {
		futureValue := FutureValue[*resource]()
		key := "key"
		res := &resource{value: 1}

		processor := batch.StartProcessor(
			batch.Options[*resource]{
				LoadResource: func(_ context.Context, actualKey string) (*resource, error) {
					require.Equal(t, key, actualKey)
					return res, nil
				},
			})
		defer processor.Stop()
		// when
		err := processor.Run(key, func(r *resource) {
			futureValue.Set(r)
		})
		require.NoError(t, err)

		assert.Same(t, res, futureValue.Get(t))
	})

	t.Run("should save modified resource", func(t *testing.T) {
		key := "key"

		db := newDatabase[resource]()
		db.SaveOrFail(t, key, &resource{value: 1})

		processor := batch.StartProcessor(
			batch.Options[*resource]{
				LoadResource: db.Load,
				SaveResource: db.Save,
			})
		defer processor.Stop()
		// when
		err := processor.Run(key, func(r *resource) {
			r.value = 2
		})
		require.NoError(t, err)

		modifiedResourceIsSaved := func() bool {
			v := db.LoadOrFail(t, key)
			return reflect.DeepEqual(v, &resource{value: 2})
		}
		assert.Eventually(t, modifiedResourceIsSaved, time.Second, time.Millisecond)
	})

	t.Run("should run batch for at least min duration", func(t *testing.T) {
		processor := batch.StartProcessor(
			batch.Options[empty]{
				MinDuration: 100 * time.Millisecond,
			},
		)
		defer processor.Stop()

		started := time.Now()
		// when
		err := processor.Run("", func(empty) {})
		require.NoError(t, err)

		elapsed := time.Now().Sub(started)
		assert.True(t, elapsed >= 100*time.Millisecond, "batch should take at least 100ms")
	})

	t.Run("should run batch with default min duration", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{})
		defer processor.Stop()

		started := time.Now()
		// when
		err := processor.Run("", func(empty) {})
		require.NoError(t, err)

		elapsed := time.Now().Sub(started)
		assert.True(t, elapsed >= 100*time.Millisecond, "batch should take 100ms by default")
	})

	t.Run("should end batch if operation took too long", func(t *testing.T) {
		var batchCount int32

		processor := batch.StartProcessor(
			batch.Options[empty]{
				MinDuration: time.Millisecond,
				MaxDuration: time.Minute,
				SaveResource: func(context.Context, string, empty) error {
					atomic.AddInt32(&batchCount, 1)
					return nil
				},
			})
		defer processor.Stop()

		key := ""

		err := processor.Run(key, func(empty) {
			time.Sleep(100 * time.Millisecond)
		})
		require.NoError(t, err)

		err = processor.Run(key, func(empty) {})
		require.NoError(t, err)

		assert.Equal(t, int32(2), atomic.LoadInt32(&batchCount))
	})

	t.Run("should abort batch", func(t *testing.T) {
		key := "key"
		timeoutError := errors.New("timeout")

		t.Run("when LoadResource returned error", func(t *testing.T) {
			customError := errors.New("error")
			db := newDatabase[resource]()
			processor := batch.StartProcessor(
				batch.Options[*resource]{
					LoadResource: func(ctx context.Context, _ string) (*resource, error) {
						return nil, customError
					},
					SaveResource: db.Save,
				},
			)
			defer processor.Stop()
			// when
			err := processor.Run(key, func(*resource) {})
			// then
			assert.ErrorIs(t, err, customError)
			// and
			db.AssertResourceNotFound(t, key)
		})

		t.Run("when LoadResource returned error after context was timed out", func(t *testing.T) {
			db := newDatabase[resource]()
			processor := batch.StartProcessor(
				batch.Options[*resource]{
					MinDuration: time.Millisecond,
					MaxDuration: time.Millisecond,
					LoadResource: func(ctx context.Context, _ string) (*resource, error) {
						select {
						case <-ctx.Done():
							return nil, timeoutError
						case <-time.After(100 * time.Millisecond):
							require.FailNow(t, "context was not timed out")
						}
						return nil, nil
					},
					SaveResource: db.Save,
				},
			)
			defer processor.Stop()
			// when
			err := processor.Run(key, func(*resource) {})
			// then
			assert.ErrorIs(t, err, timeoutError)
			// and
			db.AssertResourceNotFound(t, key)
		})

		t.Run("when SaveResource returned error after context was timed out", func(t *testing.T) {
			processor := batch.StartProcessor(
				batch.Options[empty]{
					MinDuration: time.Millisecond,
					MaxDuration: time.Millisecond,
					SaveResource: func(ctx context.Context, _ string, _ empty) error {
						select {
						case <-ctx.Done():
							return timeoutError
						case <-time.After(100 * time.Millisecond):
							require.FailNow(t, "context was not timed out")
						}
						return nil
					},
				},
			)
			defer processor.Stop()
			// when
			err := processor.Run(key, func(empty) {})
			// then
			assert.ErrorIs(t, err, timeoutError)
		})
	})

	t.Run("should run operations sequentially on a single resource (run with -race flag)", func(t *testing.T) {
		processor := batch.StartProcessor(
			batch.Options[*resource]{
				LoadResource: func(context.Context, string) (*resource, error) {
					return &resource{}, nil
				},
			},
		)
		defer processor.Stop()

		const iterations = 1000

		var group sync.WaitGroup
		group.Add(iterations)

		for i := 0; i < iterations; i++ {
			go func() {
				err := processor.Run("key", func(r *resource) {
					r.value++ // value is not guarded so data race should be reported by `go test`
				})
				require.NoError(t, err)

				group.Done()
			}()
		}

		group.Wait()
	})
}

func TestProcessor_Stop(t *testing.T) {
	t.Run("after Stop no new operation can be run", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{})
		processor.Stop()

		err := processor.Run("key", func(empty) {})
		assert.ErrorIs(t, err, batch.ProcessorStopped)
	})

	t.Run("should end running batch", func(t *testing.T) {
		minDuration := 10 * time.Second

		processor := batch.StartProcessor(
			batch.Options[empty]{
				MinDuration: minDuration,
			},
		)

		var operationExecuted sync.WaitGroup
		operationExecuted.Add(1)

		var batchFinished sync.WaitGroup
		batchFinished.Add(1)

		started := time.Now()

		go func() {
			err := processor.Run("key", func(empty) {
				operationExecuted.Done()
			})
			require.NoError(t, err)
			batchFinished.Done()
		}()
		operationExecuted.Wait()
		// when
		processor.Stop()
		// then
		batchFinished.Wait()
		elapsed := time.Now().Sub(started)
		assert.True(t, elapsed < minDuration, "stopped batch should take less time than batch min duration")
	})

	t.Run("Stop should wait until all batches are finished", func(t *testing.T) {
		var operationExecuted sync.WaitGroup
		operationExecuted.Add(1)

		batchFinished := false
		processor := batch.StartProcessor(
			batch.Options[empty]{
				MinDuration: time.Second,
				MaxDuration: time.Second,
				SaveResource: func(ctx context.Context, key string, _ empty) error {
					<-ctx.Done()
					batchFinished = true
					return nil
				},
			},
		)
		go func() {
			_ = processor.Run("key", func(empty) {
				operationExecuted.Done()
			})
		}()
		operationExecuted.Wait()
		// when
		processor.Stop()
		// then
		assert.True(t, batchFinished)
	})
}

type resource struct{ value int }
type empty struct{}
