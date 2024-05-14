package consumerLogger

import (
	"context"
	"fmt"
	"github.com/3lvia/kunde-extensions-lib-go/pkg/sfkafkalib/configuration"
	"github.com/3lvia/libraries-go/pkg/hashivault"
	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"time"
)

const (
	Local       = "local"
	Development = "dev"
)

func ConfigureLogger(env string) (*otelzap.Logger, func()) {
	var cfg zap.Config
	switch env {
	case Local, Development:
		cfg = zap.NewDevelopmentConfig()
	default:
		cfg = zap.NewProductionConfig()
	}
	cfg.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339)

	logger, err := cfg.Build()
	if err != nil {
		log.Fatalf("Unable to build consumerLogger %v", err)
	}

	ol := otelzap.New(logger)
	cleanup := otelzap.ReplaceGlobals(ol)
	return ol, cleanup
}

func NewTelemetryClient(ctx context.Context, conf *configuration.ConsumerConfig, secretsManager hashivault.SecretsManager) (appinsights.TelemetryClient, error) {
	secretFunc, err := secretsManager.GetSecret(ctx, conf.VaultIkeyPath)
	if err != nil {
		return nil, err
	}

	secretMap := secretFunc()

	return appinsights.NewTelemetryClient(fmt.Sprint(secretMap["instrumentation-key"])), nil
}
