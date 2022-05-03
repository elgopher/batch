// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package train

import "context"

// BatchProcessor is an optional interface to decouple your code from `batch` package.
type BatchProcessor interface {
	Run(ctx context.Context, key string, operation func(*Train)) error
}

type Service struct {
	BatchProcessor BatchProcessor
}

func (s Service) Book(ctx context.Context, trainName string, seatNumber int, person string) error {
	var operationError error

	batchError := s.BatchProcessor.Run(ctx, trainName, func(train *Train) {
		operationError = train.Book(seatNumber, person)
	})

	if operationError != nil {
		return operationError
	}

	return batchError
}
