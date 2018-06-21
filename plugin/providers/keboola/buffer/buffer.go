package buffer

import (
	"bytes"
	"net/url"
)

func Empty() *bytes.Buffer {
	buffer := bytes.NewBufferString("")
	return buffer
}

func FromForm(values url.Values) *bytes.Buffer {
	return bytes.NewBufferString(values.Encode())
}