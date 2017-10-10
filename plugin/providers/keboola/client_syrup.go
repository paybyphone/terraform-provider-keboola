package keboola

import (
	"bytes"
	"net/http"
)

const syrupURL = "https://syrup.keboola.com/"

func (c *KBCClient) GetFromSyrup(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", syrupURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}

func (c *KBCClient) PostToSyrup(endpoint string, jsonpayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", syrupURL+endpoint, jsonpayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}

func (c *KBCClient) PutToSyrup(endpoint string, jsonpayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", syrupURL+endpoint, jsonpayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}

func (c *KBCClient) PutFormToSyrup(endpoint string, formdata *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", syrupURL+endpoint, formdata)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	return client.Do(req)
}

func (c *KBCClient) PatchOnSyrup(endpoint string, jsonpayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PATCH", syrupURL+endpoint, jsonpayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}

func (c *KBCClient) DeleteFromSyrup(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", syrupURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}
