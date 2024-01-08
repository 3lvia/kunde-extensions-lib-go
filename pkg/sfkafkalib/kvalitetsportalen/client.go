package kvalitetsportalen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"net/http"
)

type Success struct {
	Resource       string `json:"resource"`
	HttpStatusCode int    `json:"httpstatuscode"`
	Payload        string `json:"payload"`
}

type Exception struct {
	Resource       string `json:"resource"`
	HttpStatusCode int    `json:"httpstatuscode"`
	Payload        string `json:"payload"`
	Exception      string `json:"exception"`
}

var (
	ErrResponseNotOK = errors.New("response code was not OK")
)

type Config struct {
	BaseUrl       string
	TokenEndPoint string
	ClientId      string
	ClientSecret  string
}

type Client struct {
	config Config
	client *http.Client
}

func New(c Config) (*Client, error) {
	config := &clientcredentials.Config{
		ClientID:     c.ClientId,
		ClientSecret: c.ClientSecret,
		TokenURL:     c.TokenEndPoint,
		Scopes:       nil,
		AuthStyle:    oauth2.AuthStyleAutoDetect,
	}

	return &Client{
		config: c,
		client: config.Client(context.Background()),
	}, nil
}

func (c *Client) LogSuccess(s Success) error {

	body, err := json.Marshal(s)
	if err != nil {
		return err
	}
	posturl := c.config.BaseUrl + "/LogSuccess"
	req, err := http.NewRequest("POST", posturl, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("api-version", "1.0")
	r, err := c.client.Do(req)
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return ErrResponseNotOK
	}
	return nil
}

func (c *Client) LogException(e Exception) error {

	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	posturl := c.config.BaseUrl + "/LogException"
	req, err := http.NewRequest("POST", posturl, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("api-version", "1.0")
	r, err := c.client.Do(req)

	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return ErrResponseNotOK
	}

	return nil
}

func (c *Client) url(path string) string {
	return c.config.BaseUrl + path
}