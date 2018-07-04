package keboola

import "encoding/json"

type OrchestrationTask struct {
	ID                json.Number            `json:"id"`
	Component         string                 `json:"component"`
	Action            string                 `json:"action"`
	ActionParameters  map[string]interface{} `json:"actionParameters"`
	Timeout           int                    `json:"timeoutMinutes"`
	IsActive          bool                   `json:"active"`
	ContinueOnFailure bool                   `json:"continueOnFailure"`
	Phase             string                 `json:"phase"`
}
