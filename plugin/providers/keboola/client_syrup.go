package keboola

import (
	"bytes"
	"net/http"
	"log"
	"fmt"
)

const syrupURL = "https://syrup.keboola.com/"

//GetFromSyrup requests an object from the Keboola Syrup API.
func (c *KBCClient) GetFromSyrup(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", syrupURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)

	log.Println(fmt.Sprintf("[DEBUG] GET: %s", storageURL+endpoint))

	return client.Do(req)
}

//PostToSyrup posts a new object to the Keboola Syrup API.
func (c *KBCClient) PostToSyrup(endpoint string, jsonPayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", syrupURL+endpoint, jsonPayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")

	log.Println(fmt.Sprintf("[DEBUG] POST: %s (%v bytes)", storageURL+endpoint, jsonPayload.Len()))

	return client.Do(req)
}

//PutToSyrup puts an existing object to the Keboola Syrup API for update.
func (c *KBCClient) PutToSyrup(endpoint string, jsonPayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", syrupURL+endpoint, jsonPayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")

	log.Println(fmt.Sprintf("[DEBUG] PUT: %s (%v bytes)", storageURL+endpoint, jsonPayload.Len()))

	return client.Do(req)
}

//PutFormToSyrup puts an existing object in Form encoded format to the Keboola Storage API for update.
func (c *KBCClient) PutFormToSyrup(endpoint string, formData *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", syrupURL+endpoint, formData)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/x-www-form-urlencoded")

	log.Println(fmt.Sprintf("[DEBUG] PUT: %s (%v bytes)", storageURL+endpoint, formData.Len()))

	return client.Do(req)
}

//PatchOnSyrup applies a patch/changeset to an existing object on the Keboola Storage API.
func (c *KBCClient) PatchOnSyrup(endpoint string, jsonPayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PATCH", syrupURL+endpoint, jsonPayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")

	log.Println(fmt.Sprintf("[DEBUG] PATCH: %s (%v bytes)", storageURL+endpoint, jsonPayload.Len()))

	return client.Do(req)
}

//DeleteFromSyrup removes an existing object from the Keboola Syrup API.
func (c *KBCClient) DeleteFromSyrup(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", syrupURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)

	log.Println(fmt.Sprintf("[DEBUG] DELETE: %s (%v bytes)", storageURL+endpoint))

	return client.Do(req)
}
