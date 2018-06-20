package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
)

//region Keboola API Contracts

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

//endregion

func resourceKeboolaOrchestrationTasks() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaOrchestrationTasksCreate,
		Read:   resourceKeboolaOrchestrationTasksRead,
		Update: resourceKeboolaOrchestrationTasksUpdate,
		Delete: resourceKeboolaOrchestrationTasksDelete,

		Schema: map[string]*schema.Schema{
			"orchestration_id": {
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
			"task": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"component": {
							Type:     schema.TypeString,
							Required: true,
						},
						"action": {
							Type:     schema.TypeString,
							Required: true,
						},
						"action_parameters": {
							Type:             schema.TypeString,
							Optional:         true,
							DiffSuppressFunc: suppressEquivalentJSON,
						},
						"timeout": {
							Type:     schema.TypeInt,
							Optional: true,
						},
						"is_active": {
							Type:     schema.TypeBool,
							Optional: true,
						},
						"continue_on_failure": {
							Type:     schema.TypeBool,
							Optional: true,
						},
						"phase": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func resourceKeboolaOrchestrationTasksCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Orchestration Tasks in Keboola.")

	orchestrationID := d.Get("orchestration_id").(string)
	tasks := d.Get("task").([]interface{})
	mappedTasks := make([]OrchestrationTask, 0, len(tasks))

	for _, task := range tasks {
		config := task.(map[string]interface{})

		actionParametersJSON := config["action_parameters"].(string)
		var mappedActionParameters interface{}
		json.Unmarshal([]byte(actionParametersJSON), &mappedActionParameters)

		mappedTask := OrchestrationTask{
			Component:         config["component"].(string),
			Action:            config["action"].(string),
			Timeout:           config["timeout"].(int),
			IsActive:          config["is_active"].(bool),
			ContinueOnFailure: config["continue_on_failure"].(bool),
			Phase:             config["phase"].(string),
		}

		if mappedActionParameters != nil {
			mappedTask.ActionParameters = mappedActionParameters.(map[string]interface{})
		}

		mappedTasks = append(mappedTasks, mappedTask)
	}

	tasksJSON, err := json.Marshal(mappedTasks)

	if err != nil {
		return err
	}

	client := meta.(*KBCClient)

	tasksBuffer := bytes.NewBuffer(tasksJSON)
	createTasksResponse, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", orchestrationID), tasksBuffer)

	if hasErrors(err, createTasksResponse) {
		return extractError(err, createTasksResponse)
	}

	d.SetId(orchestrationID)

	return resourceKeboolaOrchestrationTasksRead(d, meta)
}

func resourceKeboolaOrchestrationTasksRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Orchestration Tasks from Keboola.")

	if d.Id() == "" {
		return nil
	}

	orchestrationID := d.Id()

	client := meta.(*KBCClient)

	getResponse, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResponse)
	}

	var orchestrationTasks []OrchestrationTask

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&orchestrationTasks)

	if err != nil {
		return err
	}

	var tasks []map[string]interface{}

	for _, orchestrationTask := range orchestrationTasks {

		actionParametersJSON, _ := json.Marshal(orchestrationTask.ActionParameters)

		taskDetails := map[string]interface{}{
			"component":           orchestrationTask.Component,
			"action":              orchestrationTask.Action,
			"action_parameters":   string(actionParametersJSON),
			"timeout":             orchestrationTask.Timeout,
			"is_active":           orchestrationTask.IsActive,
			"continue_on_failure": orchestrationTask.ContinueOnFailure,
			"phase":               orchestrationTask.Phase,
		}

		tasks = append(tasks, taskDetails)
	}

	d.Set("orchestration_id", orchestrationID)
	d.Set("task", tasks)

	return nil
}

func resourceKeboolaOrchestrationTasksUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Orchestration Tasks in Keboola.")

	orchestrationID := d.Get("orchestration_id").(string)
	tasks := d.Get("task").([]interface{})
	mappedTasks := make([]OrchestrationTask, 0, len(tasks))

	for _, task := range tasks {
		config := task.(map[string]interface{})

		actionParametersJSON := config["action_parameters"].(string)
		var mappedActionParameters interface{}
		json.Unmarshal([]byte(actionParametersJSON), &mappedActionParameters)

		mappedTask := OrchestrationTask{
			Component:         config["component"].(string),
			Action:            config["action"].(string),
			Timeout:           config["timeout"].(int),
			IsActive:          config["is_active"].(bool),
			ContinueOnFailure: config["continue_on_failure"].(bool),
			Phase:             config["phase"].(string),
		}

		if mappedActionParameters != nil {
			mappedTask.ActionParameters = mappedActionParameters.(map[string]interface{})
		}

		mappedTasks = append(mappedTasks, mappedTask)
	}

	tasksJSON, err := json.Marshal(mappedTasks)

	if err != nil {
		return err
	}

	client := meta.(*KBCClient)

	tasksBuffer := bytes.NewBuffer(tasksJSON)
	updateResponse, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", orchestrationID), tasksBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaOrchestrationTasksRead(d, meta)
}

func resourceKeboolaOrchestrationTasksDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Orchestration Tasks in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	emptyTasksBuffer := bytes.NewBufferString("[]")
	clearTasksResponse, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", d.Id()), emptyTasksBuffer)

	if hasErrors(err, clearTasksResponse) {
		return extractError(err, clearTasksResponse)
	}

	d.SetId("")

	return nil
}
