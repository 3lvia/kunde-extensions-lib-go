package kafka

import (
	"context"
	"fmt"
	"github.com/3lvia/kunde-extensions-lib-go/pkg/sfkafkalib/configuration"
	"github.com/3lvia/libraries-go/pkg/hashivault"
	"github.com/3lvia/libraries-go/pkg/mschema"
	"net/http"
)

func CreateSchemaDescriptor(ctx context.Context, conf configuration.ConsumerConfig, secretsManager hashivault.SecretsManager) (mschema.Descriptor, error) {
	secret, err := secretsManager.GetSecret(ctx, fmt.Sprintf(conf.SchemaCredsPath, conf.System))
	if err != nil {
		return nil, err
	}
	infoSecret, err := secretsManager.GetSecret(ctx, conf.SchemaInfoPath)
	if err != nil {
		return nil, err
	}
	m := secret()
	mInfo := infoSecret()

	r, err := mschema.New(
		mInfo["schema-registry-url"].(string),
		mschema.WithClient(&http.Client{}),
		mschema.WithUser(m["schema_registry_key"].(string), m["schema_registry_secret"].(string)))
	if err != nil {
		return nil, err
	}

	d, err := r.GetBySubject(ctx, conf.Topic)
	if err != nil {
		return nil, err
	}

	return d, nil
}
