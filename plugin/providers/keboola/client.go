package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

//KbcClient is used for communicating with the Keboola Connection API
type KbcClient struct {
	APIKey string
}

//CreateResourceResult holds the results from requesting creation of a Keboola resource.
type CreateResourceResult struct {
	ID json.Number `json:"id,omitempty"`
}

const storageURL = "https://connection.keboola.com/v2/"
const syrupURL = "https://syrup.keboola.com/"
const fileImportURL = "https://import.keboola.com/"

//GetFromStorage requests an object from the Keboola Storage API.
func (c *KbcClient) GetFromStorage(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", storageURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}

//PostToStorage posts a new object to the Keboola Storage API.
func (c *KbcClient) PostToStorage(endpoint string, formdata *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", storageURL+endpoint, formdata)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	return client.Do(req)
}

//PostToFileImport posts a new object to the Keboola File Import API.
func (c *KbcClient) PostToFileImport(endpoint string, formdata *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", fileImportURL+endpoint, formdata)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "multipart/form-data; boundary=----terraform-provider-keboola----")
	return client.Do(req)
}

//PutToStorage puts an existing object to the Keboola Storage API.
func (c *KbcClient) PutToStorage(endpoint string, jsonpayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", storageURL+endpoint, jsonpayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	return client.Do(req)
}

//DeleteFromStorage removes an existing object from the Keboola Storage API.
func (c *KbcClient) DeleteFromStorage(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", storageURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}

func (c *KbcClient) GetFromSyrup(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", syrupURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}

func (c *KbcClient) PostToSyrup(endpoint string, formdata *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", syrupURL+endpoint, formdata)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}

func (c *KbcClient) PutToSyrup(endpoint string, jsonpayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", syrupURL+endpoint, jsonpayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}

func (c *KbcClient) PatchOnSyrup(endpoint string, jsonpayload *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("PATCH", syrupURL+endpoint, jsonpayload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "application/json")
	return client.Do(req)
}

func (c *KbcClient) DeleteFromSyrup(endpoint string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("DELETE", syrupURL+endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	return client.Do(req)
}

func hasErrors(err error, response *http.Response) bool {
	return err != nil || response.StatusCode < 200 || response.StatusCode > 299
}

func extractError(err error, response *http.Response) error {
	if err != nil {
		return err
	}

	contentBuffer := new(bytes.Buffer)
	contentBuffer.ReadFrom(response.Body)

	return fmt.Errorf("%v %s\n%v", response.StatusCode, contentBuffer.String(), response.Request)
}
