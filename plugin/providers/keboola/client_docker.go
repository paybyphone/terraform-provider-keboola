package keboola

import (
	"bytes"
	"net/http"
)

const dockerURL = "https://docker-runner.keboola.com/"

func (c *KBCClient) PostToDocker(endpoint string, jsonpayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", dockerURL+endpoint, jsonpayload)
	if err != nil {
		return nil, err
	}

	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}
