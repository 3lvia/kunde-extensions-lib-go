package salesforce

import (
	"context"
	"errors"
	"fmt"
	"github.com/3lvia/kunde-extensions-lib-go/pkg/sfkafkalib/configuration"
	"github.com/3lvia/libraries-go/pkg/hashivault"
)

func CreateSfConnConf(ctx context.Context, config configuration.ConsumerConfig, secretsManager hashivault.SecretsManager) (*ConnectionConfig, error) {
	secretFunc, err := secretsManager.GetSecret(ctx, config.VaultPath)
	if err != nil {
		return nil, err
	}

	secretMap := secretFunc()

	sfConnConf := new(ConnectionConfig)
	sfConnConf.LoginURL = fmt.Sprint(secretMap["salesforce-token-endpoint"])
	if sfConnConf.LoginURL == "" {
		return nil, errors.New("Missing required connection config LoginURL")
	}
	sfConnConf.ApiEndpoint = fmt.Sprint(secretMap["salesforce-api-url"])
	if sfConnConf.ApiEndpoint == "" {
		return nil, errors.New("Missing required connection config ApiEndpoint")
	}
	sfConnConf.ClientId = fmt.Sprint(secretMap["salesforce-client_id"])
	if sfConnConf.ClientId == "" {
		return nil, errors.New("Missing required connection config ClientId")
	}
	sfConnConf.ClientSecret = fmt.Sprint(secretMap["salesforce-client_secret"])
	if sfConnConf.ClientSecret == "" {
		return nil, errors.New("Missing required connection config ClientSecret")
	}
	sfConnConf.Username = fmt.Sprint(secretMap["salesforce-username"])
	if sfConnConf.Username == "" {
		return nil, errors.New("Missing required connection config Username")
	}
	sfConnConf.Password = fmt.Sprint(secretMap["salesforce-password"])
	if sfConnConf.Password == "" {
		return nil, errors.New("Missing required connection config Password")
	}
	sfConnConf.Token = fmt.Sprint(secretMap["salesforce-security-token"])
	if sfConnConf.Token == "" {
		return nil, errors.New("Missing required connection config Token")
	}

	return sfConnConf, nil
}
