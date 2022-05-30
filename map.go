package batch

import (
	"hash/fnv"
	"sync"
)

type concurrentMap[T any] struct {
	shards []*shard[T]
}

type shard[T any] struct {
	mutex         sync.Mutex
	underlyingMap map[string]T
}

func newConcurrentMap[T any](shardsNumber int) *concurrentMap[T] {
	shards := make([]*shard[T], shardsNumber)
	for i := 0; i < shardsNumber; i++ {
		shards[i] = &shard[T]{underlyingMap: map[string]T{}}
	}

	return &concurrentMap[T]{shards: shards}
}

func (c *concurrentMap[T]) FindOrCreate(key string, create func(key string) T) T {
	selectedShard := c.shard(key)

	selectedShard.mutex.Lock()
	defer selectedShard.mutex.Unlock()

	value, ok := selectedShard.underlyingMap[key]
	if ok {
		return value
	}

	value = create(key)
	selectedShard.underlyingMap[key] = value

	return value
}

func (c *concurrentMap[T]) DeleteWithCleanup(key string, cleanup func(key string)) {
	selectedShard := c.shard(key)

	selectedShard.mutex.Lock()
	defer selectedShard.mutex.Unlock()

	delete(selectedShard.underlyingMap, key)
	cleanup(key)
}

func (c *concurrentMap[T]) shard(key string) *shard[T] {
	h := fnv.New32()
	h.Write([]byte(key))
	no := int(h.Sum32()) % len(c.shards)
	return c.shards[no]
}
