package salesforce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	sObjectEndpoint = "/sobjects/KafkaMessage__c/"
)

type ApiError struct {
	err        error
	message    string
	statusCode int
	status     string
}

func (e ApiError) Error() string {
	if e.status == "" {
		return e.message
	}
	return fmt.Sprintf("%s: %s", e.message, e.status)
}

func (e ApiError) Unwrap() error {
	return e.err
}

var (
	ErrResponseNotOK = errors.New("response code was not OK")
	ErrNameIsEmpty   = errors.New("the transformer name cannot be empty")
)

type Authentication struct {
	AccessToken string `json:"access_token"`
	InstanceUrl string `json:"instance_url"`
	Id          string `json:"id"`
	TokenType   string `json:"token_type"`
	IssuedAt    string `json:"issued_at"`
	Signature   string `json:"signature"`
}

type ConnectionConfig struct {
	LoginURL     string // EG: "https://test.salesforce.com/services/oauth2/token"
	ApiEndpoint  string // EG: "/services/data/v56.0/"; In vault only version is saved, we need to add the rest of the endpoint
	ClientId     string // Connected app ClientID
	ClientSecret string // Connected app ClientSecret
}

type AuthClient struct {
	ApiUrl url.URL
	Config *clientcredentials.Config
	Ctx    context.Context
	//tracer  trace.Tracer		 //TODO Reimplement
	//service attribute.KeyValue //TODO Reimplement
}

func CreateAuthClient(ctx context.Context, conf ConnectionConfig) (authClient *AuthClient, err error) {

	authParams := url.Values{}
	credsConf := &clientcredentials.Config{
		ClientID:       conf.ClientId,
		ClientSecret:   conf.ClientSecret,
		TokenURL:       conf.LoginURL,
		EndpointParams: authParams,
		AuthStyle:      oauth2.AuthStyleInParams,
	}

	httpClient := http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

	token, err := credsConf.TokenSource(ctx).Token()
	if err != nil {
		return nil, err
	}

	instanceUrl, ok := token.Extra("instance_url").(string)
	if !ok {
		return nil, errors.New("Failed to retrieve instance URL when creating auth client")
	}

	baseUrl, err := url.Parse(instanceUrl)
	if err != nil {
		return nil, err
	}

	apiUrl, err := baseUrl.Parse(conf.ApiEndpoint + sObjectEndpoint)
	if err != nil {
		return nil, err
	}

	authClient = new(AuthClient)
	authClient.Config = credsConf
	authClient.ApiUrl = *apiUrl
	authClient.Ctx = ctx

	return authClient, nil
}

func (client *AuthClient) UpsertKafkaMessage(km KafkaMessage__c) error {
	json, err := json.Marshal(km)
	if err != nil {
		return err
	}
	fmt.Println("Message content: " + string(km.Value__c))

	req, err := http.NewRequest(http.MethodPost, client.ApiUrl.String(), bytes.NewBuffer(json))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	res, err := client.Config.Client(client.Ctx).Do(req)
	if err != nil {
		return err
	}

	fmt.Printf("Response status code from SF: '%d'", res.StatusCode)

	if !(res.StatusCode >= 200 && res.StatusCode < 300) {
		msg, err := io.ReadAll(res.Body)
		if err != nil {
			msg = []byte("")
		}
		return ApiError{
			err:        ErrResponseNotOK,
			message:    "Unable to post kafka message to Salesforce with message: " + string(msg),
			statusCode: res.StatusCode,
			status:     res.Status,
		}
	}

	return nil
}
