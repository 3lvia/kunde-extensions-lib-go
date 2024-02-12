package kafka

import (
	"context"

	"github.com/3lvia/kunde-extensions-lib-go/pkg/sfkafkalib/salesforce"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.opentelemetry.io/otel/trace"
)

type HandlerFunc func(context.Context, salesforce.KafkaMessage__c) error

func HandleFunc(tracer trace.Tracer, logger *otelzap.Logger, sfAuthClient *salesforce.AuthClient) HandlerFunc {
	return func(ctx context.Context, message salesforce.KafkaMessage__c) error {

		err := sfAuthClient.UpsertKafkaMessage(message)
		if err != nil {
			logger.Sugar().ErrorwContext(ctx, "upsert to salesforce failed", "key", message.Key__c)
			panic(message)
		} else {
			logger.Sugar().InfowContext(ctx, "upserted message", "key", message.Key__c)
		}
		return err
	}
}
