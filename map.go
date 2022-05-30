package batch

import "sync"

type concurrentMap[T any] struct {
	mutex         sync.Mutex
	underlyingMap map[string]T
}

func newConcurrentMap[T any]() *concurrentMap[T] {
	return &concurrentMap[T]{underlyingMap: map[string]T{}}
}

func (c *concurrentMap[T]) FindOrCreate(key string, create func(key string) T) T {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	value, ok := c.underlyingMap[key]
	if ok {
		return value
	}

	value = create(key)
	c.underlyingMap[key] = value

	return value
}

func (c *concurrentMap[T]) DeleteWithCleanup(key string, cleanup func(key string)) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.underlyingMap, key)
	cleanup(key)
}
