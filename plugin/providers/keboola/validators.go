package keboola

import "fmt"

func validateAccessTokenBucketPermissions(v interface{}, k string) (ws []string, errors []error) {
	values := AsStringArray(v.([]interface{}))
	for _, value := range values {
		if value != "read" && value != "write" && value != "manage" {
			errors = append(errors, fmt.Errorf(
				"%q must be set to one of %s, %s or %s, got %q",
				k, "read", "write", "manage", value))
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
