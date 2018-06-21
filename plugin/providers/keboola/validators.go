package keboola

import (
	"fmt"
	"strings"
)

func validateAccessTokenBucketPermissions(v interface{}, k string) (ws []string, errors []error) {
	values := v.(map[string]interface{})
	for key, value := range values {
		if value != "read" && value != "write" && value != "manage" {
			errors = append(errors, fmt.Errorf(
				"%q must be set to one of %s, %s or %s, got %q for '%q'",
				k, "read", "write", "manage", value, key))
		}
	}

	return
}

func validateStorageBucketStage(v interface{}, k string) (ws []string, errors []error) {
	value := v.(string)
	if value != "in" && value != "out" && value != "sys" {
		errors = append(errors, fmt.Errorf(
			"%q must be set to one of %s, %s or %s, got %q",
			k, "in", "out", "sys", value))
	}

	return
}

func validateStorageBucketBackend(v interface{}, k string) (ws []string, errors []error) {
	if value := v.(string); value != "" {
		if value != "snowflake" && value != "mysql" && value != "redshift" {
			errors = append(errors, fmt.Errorf(
				"%q must be set to one of %s, %s or %s, got %q",
				k, "snowflake", "mysql", "redshift", value))
		}
	}

	return
}

func validateOrchestrationNotificationChannel(v interface{}, k string) (ws []string, errors []error) {
	value := v.(string)
	if value != "error" && value != "warning" && value != "processing" {
		errors = append(errors, fmt.Errorf(
			"%q must be set to one of %s, %s or %s, got %q",
			k, "error", "warning", "processing", value))
	}

	return
}

func validateKBCEncryptedValue(v interface{}, k string) (ws []string, errors []error) {
	value := v.(string)
	if !strings.HasPrefix(value, "KBC::ProjectSecure::") {
		errors = append(errors, fmt.Errorf(
			"%q must be a value encrypted using the KBC Encryption API (https://developers.keboola.com/overview/encryption/), and is expected to start with 'KBC::ProjectSecure::'", k))
	}

	return
}