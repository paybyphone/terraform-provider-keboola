package keboola

import (
	"bytes"
	"net/http"
)

const fileImportURL = "https://import.keboola.com/"

//PostToFileImport posts a new object to the Keboola File Import API.
func (c *KBCClient) PostToFileImport(endpoint string, formdata *bytes.Buffer) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("POST", fileImportURL+endpoint, formdata)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-StorageApi-Token", c.APIKey)
	req.Header.Add("content-type", "multipart/form-data; boundary=----terraform-provider-keboola----")
	return client.Do(req)
}
