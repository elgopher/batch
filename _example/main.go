// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package main

import (
	"time"

	"github.com/elgopher/batch"
	"github.com/elgopher/batch/_example/http"
	"github.com/elgopher/batch/_example/logger"
	"github.com/elgopher/batch/_example/store"
	"github.com/elgopher/batch/_example/train"
)

func main() {
	db := store.File{Dir: "/tmp/"}

	processor := batch.StartProcessor(
		batch.Options[*train.Train]{
			MinDuration:  100 * time.Millisecond,
			MaxDuration:  3 * time.Second,
			LoadResource: db.LoadTrain,
			SaveResource: db.SaveTrain,
		},
	)
	defer processor.Stop()

	go logger.LogMetrics(processor.SubscribeBatchMetrics())

	trainService := train.Service{
		BatchProcessor: processor,
	}

	if err := http.ListenAndServe(trainService); err != nil {
		panic(err)
	}
}
