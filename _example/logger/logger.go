// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package logger

import (
	"fmt"
	"time"

	"github.com/elgopher/batch"
)

func LogMetrics(metrics <-chan batch.Metric) {
	var operationCount, batchCount int

	eachSecond := time.NewTicker(time.Second)
	defer eachSecond.Stop()

	for {
		select {
		case metric, ok := <-metrics:
			if !ok {
				// channel is closed after processor was stopped
				return
			}

			operationCount += metric.OperationCount
			batchCount += 1

		case <-eachSecond.C:
			fmt.Println(operationCount, "operations in", batchCount, "batches so far")
		}
	}
}
