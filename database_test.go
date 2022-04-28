// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type database[Value any] struct {
	mutex      sync.Mutex
	valueByKey map[string]Value
}

func newDatabase[Value any]() *database[Value] {
	return &database[Value]{
		valueByKey: map[string]Value{},
	}
}

func (d *database[Value]) Load(_ context.Context, key string) (*Value, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	v, found := d.valueByKey[key]
	if !found {
		return nil, fmt.Errorf("key %v not found", key)
	}

	return &v, nil
}

func (d *database[Value]) LoadOrFail(t *testing.T, key string) *Value {
	v, err := d.Load(context.Background(), key)
	require.NoError(t, err)

	return v
}

func (d *database[Value]) AssertResourceNotFound(t *testing.T, key string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	_, found := d.valueByKey[key]
	assert.Falsef(t, found, "resource with key %v was found but should not", key)
}

func (d *database[Value]) Save(_ context.Context, key string, v *Value) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.valueByKey[key] = *v

	return nil
}

func (d *database[Value]) SaveOrFail(t *testing.T, key string, v *Value) {
	err := d.Save(context.Background(), key, v)
	require.NoError(t, err)
}
