// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/elgopher/batch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessor_SubscribeBatchMetrics(t *testing.T) {
	const key = "key"
	ctx := context.Background()

	t.Run("should get closed channel when subscribing metrics on stopped processor", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{})
		processor.Stop()
		// when
		_, ok := <-processor.SubscribeBatchMetrics()
		// then
		assert.False(t, ok, "metrics channel should be closed")
	})

	t.Run("subscription should get batch metrics", func(t *testing.T) {
		var (
			loadResourceDuration = 100 * time.Millisecond
			saveResourceDuration = 50 * time.Millisecond
			operationDuration    = 10 * time.Millisecond
			totalDuration        = loadResourceDuration + saveResourceDuration + operationDuration
		)
		processor := batch.StartProcessor(batch.Options[empty]{
			MinDuration: loadResourceDuration,
			LoadResource: func(_ context.Context, key string) (empty, error) {
				time.Sleep(loadResourceDuration)
				return empty{}, nil
			},
			SaveResource: func(_ context.Context, key string, r empty) error {
				time.Sleep(saveResourceDuration)
				return nil
			},
		})
		defer processor.Stop()
		subscription := processor.SubscribeBatchMetrics()
		err := processor.Run(ctx, key, func(empty) {
			time.Sleep(operationDuration)
		})
		require.NoError(t, err)
		// when
		metric := <-subscription
		// then
		assert.Equal(t, "key", metric.ResourceKey)
		assert.NotZero(t, metric.BatchStart)
		assert.Equal(t, metric.OperationCount, 1)
		assertDurationInDelta(t, totalDuration, metric.TotalDuration, 10*time.Millisecond)
		assertDurationInDelta(t, loadResourceDuration, metric.LoadResourceDuration, 10*time.Millisecond)
		assertDurationInDelta(t, saveResourceDuration, metric.SaveResourceDuration, 10*time.Millisecond)
		assert.NoError(t, metric.Error)
	})

	deliberateError := errors.New("fail")

	t.Run("should get error in metric when LoadResource failed", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{
			MinDuration: time.Millisecond,
			LoadResource: func(_ context.Context, key string) (empty, error) {
				return empty{}, deliberateError
			},
		})
		metrics := processor.SubscribeBatchMetrics()
		err := processor.Run(ctx, key, func(empty) {})
		require.Error(t, err)
		// when
		metric := <-metrics
		// then
		assert.ErrorIs(t, metric.Error, deliberateError)
	})

	t.Run("should get error in metric when SaveResource failed", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{
			MinDuration: time.Millisecond,
			SaveResource: func(_ context.Context, key string, _ empty) error {
				return deliberateError
			},
		})
		metrics := processor.SubscribeBatchMetrics()
		err := processor.Run(ctx, key, func(empty) {})
		require.Error(t, err)
		// when
		metric := <-metrics
		// then
		assert.ErrorIs(t, metric.Error, deliberateError)
	})

	t.Run("each subscription should get all batch metrics", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{
			MinDuration: time.Millisecond,
		})
		defer processor.Stop()
		subscription1 := processor.SubscribeBatchMetrics()
		subscription2 := processor.SubscribeBatchMetrics()
		err := processor.Run(ctx, key, func(empty) {})
		require.NoError(t, err)
		// when
		metrics1 := <-subscription1
		metrics2 := <-subscription2
		// then
		assert.Equal(t, metrics1, metrics2)
	})

	t.Run("stopping processor should close subscription", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{
			MinDuration: time.Millisecond,
		})
		subscription := processor.SubscribeBatchMetrics()
		// when
		processor.Stop()
		_, ok := <-subscription
		// then
		assert.False(t, ok, "metrics channel should be closed")
	})

	t.Run("should not data race when run with -race flag", func(t *testing.T) {
		processor := batch.StartProcessor(batch.Options[empty]{
			MinDuration: time.Millisecond,
		})
		defer processor.Stop()

		var g sync.WaitGroup
		const goroutines = 1000
		g.Add(goroutines)

		for i := 0; i < goroutines/2; i++ {
			go func() {
				metrics := processor.SubscribeBatchMetrics()
				g.Done()
				for range metrics {
				}
			}()

			k := fmt.Sprintf("%d", i)
			go func() {
				err := processor.Run(ctx, k, func(empty) {})
				require.NoError(t, err)
				g.Done()
			}()
		}

		g.Wait()
	})
}

func assertDurationInDelta(t *testing.T, expected time.Duration, actual time.Duration, delta time.Duration) {
	diff := expected - actual
	if diff < 0 {
		diff *= -1
	}
	if diff > delta {
		require.Failf(t, "invalid duration", "actual duration %s different than expected %s (max difference is %s)", actual, expected, delta)
	}
}
