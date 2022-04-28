// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package train

type ErrValidation string

func (e ErrValidation) Error() string {
	return string(e)
}

func (e ErrValidation) Is(err error) bool {
	_, ok := err.(ErrValidation)
	return ok
}
