package kafka

import "github.com/3lvia/libraries-go/pkg/kafkaclient"

type kafkaFilter struct {
	keep filterFn
}

type filterFn func(message *kafkaclient.StreamingMessage) bool
