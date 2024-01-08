package kafka

import (
	"context"

	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.opentelemetry.io/otel/trace"
	"sfkafkalib/salesforce"
)

type HandlerFunc func(context.Context, salesforce.KafkaMessage__c) error

func HandleFunc(tracer trace.Tracer, logger *otelzap.Logger, sfAuthClient *salesforce.AuthClient) HandlerFunc {
	return func(ctx context.Context, message salesforce.KafkaMessage__c) error {

		err := sfAuthClient.UpsertKafkaMessage(ctx, message) // TODO READD THIS
		if err != nil {
			logger.Sugar().ErrorwContext(ctx, "upsert to salesforce failed", "key", message.Key__c)
		} else {
			logger.Sugar().InfowContext(ctx, "upserted message", "key", message.Key__c)
		}
		return err
	}
}
