package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

//KBCClient is used for communicating with the Keboola Connection API
type KBCClient struct {
	APIKey string
}

//CreateResourceResult holds the results from requesting creation of a Keboola resource.
type CreateResourceResult struct {
	ID json.Number `json:"id,omitempty"`
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
