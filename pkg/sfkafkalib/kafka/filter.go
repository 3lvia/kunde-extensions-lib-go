package kafka

import "github.com/3lvia/libraries-go/pkg/kafkaclient"

type Filter struct {
	keep filterFn
}

type filterFn func(m *kafkaclient.StreamingMessage) bool
