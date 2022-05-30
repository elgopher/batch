// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/elgopher/batch"
)

func BenchmarkProcessor_Run(b *testing.B) {
	resources := []int{
		1, 8, 64, 512, 4096, 32768, 262144, 2097152,
	}

	for _, resourceCount := range resources {
		b.Run(strconv.Itoa(resourceCount), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			processor := batch.StartProcessor(batch.Options[empty]{})
			defer processor.Stop()

			var allOperationsFinished sync.WaitGroup
			allOperationsFinished.Add(b.N)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := strconv.Itoa(i % resourceCount)
				go func() {
					// when
					if err := processor.Run(context.Background(), key, operation); err != nil {
						panic(err)
					}
					allOperationsFinished.Done()
				}()
			}

			b.StopTimer()

			allOperationsFinished.Wait()
		})
	}
}

func operation(empty) {}
