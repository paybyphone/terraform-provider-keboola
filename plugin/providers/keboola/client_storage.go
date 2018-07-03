package keboola

import (
	"bytes"
	"net/http"
)

const storageURL = "https://connection.keboola.com/v2/"

//GetFromStorage requests an object from the Keboola Storage API.
func (c *KBCClient) GetFromStorage(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", storageURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}

//PostToStorage posts a new object to the Keboola Storage API.
func (c *KBCClient) PostToStorage(endpoint string, formdata *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", storageURL+endpoint, formdata)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	return client.Do(req)
}

//PutToStorage puts an existing object to the Keboola Storage API for update.
func (c *KBCClient) PutToStorage(endpoint string, formData *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", storageURL+endpoint, formData)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	return client.Do(req)
}

//DeleteFromStorage removes an existing object from the Keboola Storage API.
func (c *KBCClient) DeleteFromStorage(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", storageURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}
