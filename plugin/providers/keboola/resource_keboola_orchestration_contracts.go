package keboola

import "encoding/json"

type OrchestrationNotification struct {
	Email      string                 `json:"email"`
	Channel    string                 `json:"channel"`
	Parameters map[string]interface{} `json:"parameters"`
}

type OrchestrationToken struct {
	ID          string `json:"id"`
	Description string `json:"description"`
}

type Orchestration struct {
	ID            json.Number                 `json:"id,omitempty"`
	Name          string                      `json:"name"`
	ScheduleCRON  string                      `json:"crontabRecord"`
	Token         OrchestrationToken          `json:"token,omitempty"`
	Notifications []OrchestrationNotification `json:"notifications"`
}
