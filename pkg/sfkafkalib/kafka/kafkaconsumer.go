package kafka

import (
	"context"
	"elvia.io/jordfeil-consumer/salesforce"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/3lvia/libraries-go/pkg/hashivault"
	"github.com/3lvia/libraries-go/pkg/kafkaclient"
	"github.com/3lvia/libraries-go/pkg/mschema"
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

type ConsumerConfig struct {
	System                   string
	Topic                    string
	Application              string
	SchemaInfoPath           string
	SchemaCredsPath          string
	VaultPath                string
	TraceInstrumentationName string
}

type Consumer struct {
	logger  *otelzap.Logger
	tracer  trace.Tracer
	ctx     context.Context
	conf    ConsumerConfig
	cancel  context.CancelFunc
	outChan chan salesforce.KafkaMessage__c
	errChan chan error
	desc    mschema.Descriptor
}

func CreateKafkaConsumer(ctx context.Context, conf *ConsumerConfig, log *otelzap.Logger, secretsManager hashivault.SecretsManager, sfAuthClient *salesforce.AuthClient) (context.CancelFunc, error) {
	d, err := CreateSchemaDescriptor(ctx, *conf, secretsManager)
	if err != nil {
		fmt.Println("Could not create schema descriptor, error: " + err.Error())
		return nil, err
	}

	fmt.Println("CREATE KAFKA CONSUMER")
	tracer := otel.Tracer(conf.TraceInstrumentationName)

	consumer, outChan, errChan, err := newConsumer(ctx, *conf, log, tracer, secretsManager, d)

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
				if err := handleFunc(ctx, message); err != nil {
					log.Sugar().ErrorwContext(ctx, "error in kafka handler", zap.Error(err))
				}
			case err := <-errChan:
				fmt.Println("ERROR")
				log.Sugar().ErrorwContext(ctx, "error in kafka handler", zap.Error(err))
			}
		}
	}(ctx, outChan, errChan)

	return func() {
		cancel()
		consumer.Shutdown()
	}, err
}

func newConsumer(ctx context.Context, conf ConsumerConfig, l *otelzap.Logger, tracer trace.Tracer, v hashivault.SecretsManager, d mschema.Descriptor) (*Consumer, <-chan salesforce.KafkaMessage__c, <-chan error, error) {

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
		logger:  l,
		tracer:  tracer,
		ctx:     ctx,
		conf:    conf,
		cancel:  cancel,
		outChan: outChan,
		errChan: errChan,
		desc:    d,
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

	key := string(msg.Key)
	span.SetAttributes(attribute.String("key", key))
	c.logger.Sugar().InfowContext(ctx, "received a message", "key", key)

	dto, err := c.unmarshal(msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.logger.Sugar().ErrorwContext(ctx, "unable to consume message", "key", key, zap.Error(err))
		errChan <- err
		return
	}

	outChan <- *dto

	span.SetStatus(codes.Ok, "message consumed") // TODO: Potential error where we send OK but sending to SF fails -> Lost message?
	c.logger.Sugar().InfowContext(ctx, "message consumed", "key", key)
}

func (c *Consumer) unmarshal(msg *kafkaclient.StreamingMessage) (*salesforce.KafkaMessage__c, error) {
	fmt.Println("UNMARSHAL INITIAL STRING VALUE:")
	fmt.Println(msg.Value)
	switch c.desc.Type() {
	case mschema.AVRO:
		if v, isMap := msg.Value.(map[string]interface{}); isMap {
			b, err := json.Marshal(unmarshalAvroStrings(v))
			if err != nil {
				return nil, err
			}
			fmt.Println("STRING VALUE:\n" + string(b))
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
