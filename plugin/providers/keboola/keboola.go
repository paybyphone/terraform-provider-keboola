package keboola

import (
	"fmt"
	"strings"
	"time"
	"strconv"
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
		return fmt.Errorf("error unmarshaling to boolean: invalid input %s", asString)
	}
	return nil
}

//KBCNumberString represents a dual value in the Keboola APIs, which can take either a string, or a number/integer.
type KBCNumberString string

//UnmarshalJSON handles unmarshaling a KBCNumberString in to JSON.
func (kns *KBCNumberString) UnmarshalJSON(data []byte) error {
	asString := string(data)
	*kns = KBCNumberString(asString)
	return nil
}

//KBCBooleanNumber represents a dual value in the Keboola APIs, which can take either a boolean, or a number/integer.
type KBCBooleanNumber int

//UnmarshalJSON handles unmarshaling a KBCNumberString in to JSON.
func (kbn *KBCBooleanNumber) UnmarshalJSON(data []byte) error {
	asString := string(data)

	if asString == "true" {
		*kbn = 1
	} else if asString == "false" {
		*kbn = 0
	} else if val, err := strconv.Atoi(asString); err == nil {
		*kbn = KBCBooleanNumber(val)
	} else {
		return fmt.Errorf("error unmarshaling to boolean/integer: invalid input %s", asString)
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
