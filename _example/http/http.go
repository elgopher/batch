// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/elgopher/batch/_example/train"
)

type TrainService interface {
	Book(ctx context.Context, train string, seatNumber int, person string) error
}

func ListenAndServe(trainService TrainService) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/book", bookHandler(trainService))

	server := &http.Server{Addr: ":8080", Handler: mux}
	return server.ListenAndServe()
}

// example request: /book?train=batchy&person=Jacek&seat=3
func bookHandler(trainService TrainService) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		trainKey := request.Form.Get("train")
		person := request.Form.Get("person")
		seat, err := strconv.Atoi(request.Form.Get("seat"))
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte("invalid seat number"))
			return
		}

		err = trainService.Book(request.Context(), trainKey, seat, person)

		if errors.Is(err, train.ErrValidation("")) {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte(err.Error()))
			return
		}

		if err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			fmt.Println("internal server error:", err)
			return
		}

		writer.WriteHeader(http.StatusOK)
	}
}
