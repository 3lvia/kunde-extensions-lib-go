package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/3lvia/kunde-extensions-lib-go/pkg/sfkafkalib/configuration"
	"github.com/3lvia/kunde-extensions-lib-go/pkg/sfkafkalib/salesforce"
	"github.com/3lvia/libraries-go/pkg/hashivault"
	"github.com/3lvia/libraries-go/pkg/kafkaclient"
	"github.com/3lvia/libraries-go/pkg/mschema"
	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/microsoft/ApplicationInsights-Go/appinsights/contracts"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var (
	ErrUnexpectedData        = errors.New("unexpected validData type of value in message")
	ErrDeserializationFailed = errors.New("could not deserialize value")
)

type Consumer struct {
	logger    *otelzap.Logger
	telemetry *appinsights.TelemetryClient
	filter    *Filter
	tracer    trace.Tracer
	ctx       context.Context
	conf      configuration.ConsumerConfig
	cancel    context.CancelFunc
	outChan   chan salesforce.KafkaMessage__c
	errChan   chan error
	desc      mschema.Descriptor
}

func CreateKafkaConsumer(ctx context.Context, conf *configuration.ConsumerConfig, log *otelzap.Logger, t *appinsights.TelemetryClient, filter *Filter, secretsManager hashivault.SecretsManager, sfAuthClient *salesforce.AuthClient) (context.CancelFunc, error) {
	d, err := CreateSchemaDescriptor(ctx, *conf, secretsManager)
	if err != nil {
		fmt.Println("Could not create schema descriptor, error: " + err.Error())
		return nil, err
	}

	fmt.Println("CREATE KAFKA CONSUMER")
	tracer := otel.Tracer(conf.TraceInstrumentationName)

	consumer, outChan, errChan, err := newConsumer(ctx, *conf, log, t, filter, tracer, secretsManager, d)

	if err != nil {
		fmt.Println("Could not create new Conumer, error: " + err.Error())
		return nil, err
	}

	if consumer == nil {
		fmt.Println("CONSUMER IS NIL")
	}

	handleFunc := HandleFunc(tracer, log, sfAuthClient)

	ctx, cancel := context.WithCancel(ctx)
	go func(ctx context.Context, oc <-chan salesforce.KafkaMessage__c, ec <-chan error) {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("DONE")
				return
			case message := <-oc:
				fmt.Println("CONSUMING MESSAGE: " + message.Key__c)
				(*t).TrackTrace("Consuming KafkaMessage: "+message.Key__c, contracts.Verbose)
				if err := handleFunc(ctx, message); err != nil {
					log.Sugar().ErrorwContext(ctx, "error in kafka handler", zap.Error(err))
					(*t).TrackException(err)
					panic(message)
				}
			case err := <-errChan:
				fmt.Println("ERROR")
				(*t).TrackException(err)
				log.Sugar().ErrorwContext(ctx, "error in kafka handler", zap.Error(err))
				panic(err)
			}
		}
	}(ctx, outChan, errChan)

	return func() {
		cancel()
		consumer.Shutdown()
	}, err
}

func newConsumer(ctx context.Context, conf configuration.ConsumerConfig, l *otelzap.Logger, t *appinsights.TelemetryClient, f *Filter, tracer trace.Tracer, v hashivault.SecretsManager, d mschema.Descriptor) (*Consumer, <-chan salesforce.KafkaMessage__c, <-chan error, error) {

	opts := []kafkaclient.Option{
		kafkaclient.WithSecretsManager(v),
		kafkaclient.UseAVRO(),
	}

	fmt.Println("START CONSUMER")

	stream, err := kafkaclient.StartConsumer(ctx, conf.System, conf.Topic, conf.Application, opts...)
	if err != nil {
		return nil, nil, nil, err
	}

	outChan := make(chan salesforce.KafkaMessage__c)
	errChan := make(chan error)

	ctx, cancel := context.WithCancel(ctx)

	c := &Consumer{
		logger:    l,
		filter:    f,
		tracer:    tracer,
		telemetry: t,
		ctx:       ctx,
		conf:      conf,
		cancel:    cancel,
		outChan:   outChan,
		errChan:   errChan,
		desc:      d,
	}

	go c.consume(stream)

	return c, c.outChan, c.errChan, nil
}

func (c *Consumer) Shutdown() {
	c.cancel()
	close(c.errChan)
	close(c.outChan)
}

func (c *Consumer) consume(s <-chan *kafkaclient.StreamingMessage) {
	ctx, span := c.tracer.Start(c.ctx, "kafka.consume", trace.WithAttributes(semconv.PeerService("KAFKA")))
	defer span.End()

	fmt.Println("CONSUME")

	for {
		select {
		case <-c.ctx.Done():
			fmt.Println("DONE")
			c.logger.InfoContext(ctx, "kafka consumer shutting down")
			return
		case msg := <-s:
			fmt.Println("CASE MSG")
			if msg.Error != nil {
				(*c.telemetry).TrackException(msg.Error)
				c.logger.ErrorContext(ctx, "kafka consumer error", zap.Error(msg.Error))
			} else {
				fmt.Println("RECEIVE")
				c.receive(ctx, msg, c.outChan, c.errChan)
			}
		}
	}
}

func (c *Consumer) receive(ctx context.Context, msg *kafkaclient.StreamingMessage, outChan chan salesforce.KafkaMessage__c, errChan chan error) {
	ctx, span := c.tracer.Start(ctx, "kafka.receive", trace.WithAttributes(semconv.PeerService("KAFKA")))
	defer span.End()

	if c.filter != nil && !c.filter.keep(msg) {
		span.SetStatus(codes.Ok, "message filtered")
	} else {
		key := string(msg.Key)
		span.SetAttributes(attribute.String("key", key))
		(*c.telemetry).TrackTrace("received a message with key "+key, contracts.Verbose)
		c.logger.Sugar().InfowContext(ctx, "received a message", "key", key)

		dto, err := c.unmarshal(msg)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			(*c.telemetry).TrackException(err)
			c.logger.Sugar().ErrorwContext(ctx, "unable to consume message", "key", key, zap.Error(err))
			errChan <- err
			return
		}

		outChan <- *dto

		span.SetStatus(codes.Ok, "message consumed") // TODO: Potential error where we send OK but sending to SF fails -> Lost message?
		c.logger.Sugar().InfowContext(ctx, "message consumed", "key", key)
	}
}

func (c *Consumer) unmarshal(msg *kafkaclient.StreamingMessage) (*salesforce.KafkaMessage__c, error) {
	switch c.desc.Type() {
	case mschema.AVRO:
		if v, isMap := msg.Value.(map[string]interface{}); isMap {
			b, err := json.Marshal(unmarshalAvroStrings(v))
			if err != nil {
				return nil, err
			}
			return &salesforce.KafkaMessage__c{
				Key__c:   string(msg.Key),
				Topic__c: c.conf.Topic,
				Value__c: b,
			}, nil
		} else {
			return nil, ErrDeserializationFailed
		}
	default:
		return nil, ErrUnexpectedData
	}
}

/*
Changes avro string fields from the form map[someKey:map[string:actualValue]] to map[somekey:actualValue].
This is necessary for the parsing to json to work correctly.
Works recursively so should handle any such occurence however far down into the structure it goes.
*/
func unmarshalAvroStrings(obj any) any {
	if m, objIsMap := obj.(map[string]interface{}); objIsMap {
		for k, v := range m {
			if valueMap, valueIsMap := v.(map[string]interface{}); valueIsMap {
				if str, isString := valueMap["string"]; isString {
					m[k] = str
				} else {
					m[k] = unmarshalAvroStrings(valueMap)
				}
			}
		}
		return m
	}
	return obj
}
