package keboola

import (
	"fmt"
	"strings"
	"time"
)

//KBCBoolean represents a boolean value in the Keboola APIs, which can take a 0/1 or false/true value.
type KBCBoolean bool

//UnmarshalJSON handles unmarshaling a KBCBoolean in to JSON.
func (bit *KBCBoolean) UnmarshalJSON(data []byte) error {
	asString := string(data)
	if asString == "1" || asString == "true" {
		*bit = true
	} else if asString == "0" || asString == "false" {
		*bit = false
	} else {
		return fmt.Errorf("Boolean unmarshal error: invalid input %s", asString)
	}
	return nil
}

//KBCTime represents a time value in the Keboola APIs, which has a very specific formatting.
type KBCTime struct {
	time.Time
}

//UnmarshalJSON handles unmarshaling a KBCTime in to JSON.
func (kt *KBCTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")

	if s == "null" {
		kt.Time = time.Time{}
		return
	}

	kt.Time, err = time.Parse("2006-01-02T15:04:05-0700", s)

	return
}
