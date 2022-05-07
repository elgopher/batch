// (c) 2022 Jacek Olszak
// This code is licensed under MIT license (see LICENSE for details)

package batch

import "sync"

const metricBufferSize = 1024

type metricBroker struct {
	mutex         sync.Mutex
	subscriptions []chan Metric
}

func (s *metricBroker) subscribe() <-chan Metric {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	subscription := make(chan Metric, metricBufferSize)
	s.subscriptions = append(s.subscriptions, subscription)

	return subscription
}

func (s *metricBroker) publish(metric Metric) {
	s.mutex.Lock()
	subscriptionsCopy := make([]chan Metric, len(s.subscriptions))
	copy(subscriptionsCopy, s.subscriptions)
	s.mutex.Unlock()

	for _, subscription := range subscriptionsCopy {
		subscription <- metric
	}
}

func (s *metricBroker) stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, subscription := range s.subscriptions {
		close(subscription)
	}
}
