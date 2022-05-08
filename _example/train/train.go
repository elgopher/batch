// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package train

import (
	"fmt"
)

type Train struct {
	Seats []string
}

func New(maxSeats int) *Train {
	return &Train{
		Seats: make([]string, maxSeats),
	}
}

func (t *Train) Book(seatNumber int, person string) error {
	// first validate if action is possible (`try phase` of a method)
	// such validation is needed to always keep Train in a consistent state
	// (data consistency is required to properly handle next request in a batch)
	if err := t.validateBooking(seatNumber, person); err != nil {
		return err
	}

	// then mutate state (`do phase` of method)
	t.Seats[seatNumber] = person

	return nil
}

func (t *Train) validateBooking(seatNumber int, person string) error {
	if seatNumber < 0 || seatNumber >= len(t.Seats) {
		return ErrValidation(fmt.Sprintf("train does not have seat number %d", seatNumber))
	}

	if person == "" {
		return ErrValidation("empty person name")
	}

	for n, seat := range t.Seats {
		if seat == person && seatNumber != n {
			return ErrValidation("person already booked a different seat number in the train")
		}
	}

	if t.Seats[seatNumber] != "" && t.Seats[seatNumber] != person {
		return ErrValidation("seat number is already booked by another person")
	}

	return nil
}
