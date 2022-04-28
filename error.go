// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import "errors"

var ProcessorStopped = errors.New("run failed: processor is stopped")
