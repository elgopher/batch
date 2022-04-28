// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import (
	"hash/fnv"
)

func GoroutineNumberForKey(key string, goroutines int) int {
	hash32 := fnv.New32()
	_, _ = hash32.Write([]byte(key))
	sum := hash32.Sum32()
	return int(sum) % goroutines
}
