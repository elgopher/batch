// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package store

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"

	"github.com/elgopher/batch/_example/train"
)

// File is database implementation which stores data to a file. Real world app would use real database instead, which supports
// atomic write with some kind of optimistic locking (version-check, compare and swap etc.) to always have data in consistent state.
type File struct {
	Dir string
}

func (d File) LoadTrain(_ context.Context, key string) (*train.Train, error) {
	jason, err := ioutil.ReadFile(d.filename(key))
	if os.IsNotExist(err) {
		return train.New(30), nil
	}

	t := &train.Train{}
	err = json.Unmarshal(jason, t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (d File) filename(key string) string {
	return path.Join(d.Dir, key+".json")
}

func (d File) SaveTrain(_ context.Context, key string, t *train.Train) error {
	jason, err := json.Marshal(t)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(d.filename(key), jason, 0644)
}
