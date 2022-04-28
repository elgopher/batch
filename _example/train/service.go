// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package train

// BatchProcessor is an optional interface to decouple your code from `batch` package.
type BatchProcessor interface {
	Run(key string, operation func(*Train)) error
}

type Service struct {
	BatchProcessor BatchProcessor
}

func (s Service) Book(trainName string, seatNumber int, person string) error {
	var operationError error

	batchError := s.BatchProcessor.Run(trainName, func(train *Train) {
		operationError = train.Book(seatNumber, person)
	})

	if operationError != nil {
		return operationError
	}

	return batchError
}
