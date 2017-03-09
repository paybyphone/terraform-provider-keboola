package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
)

type OrchestrationTask struct {
	ID                int                    `json:"id"`
	Component         string                 `json:"component"`
	Action            string                 `json:"action"`
	ActionParameters  map[string]interface{} `json:"actionParameters"`
	Timeout           int                    `json:"timeoutMinutes"`
	IsActive          bool                   `json:"active"`
	ContinueOnFailure bool                   `json:"continueOnFailure"`
	Phase             string                 `json:"phase"`
}

func resourceKeboolaOrchestrationTasks() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaOrchestrationTasksCreate,
		Read:   resourceKeboolaOrchestrationTasksRead,
		Update: resourceKeboolaOrchestrationTasksUpdate,
		Delete: resourceKeboolaOrchestrationTasksDelete,

		Schema: map[string]*schema.Schema{
			"orchestration_id": &schema.Schema{
				Type:     schema.TypeString,
				ForceNew: true,
				Required: true,
			},
			"task": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"component": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"action": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"actionParameters": &schema.Schema{
							Type:     schema.TypeMap,
							Optional: true,
						},
						"timeout": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
						"isActive": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
						},
						"continueOnFailure": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
						},
						"phase": &schema.Schema{
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
	log.Print("[INFO] Creating Orchestration Task in Keboola.")

	orchestrationID := d.Get("orchestration_id").(string)
	tasks := d.Get("task").([]interface{})
	mappedTasks := make([]OrchestrationTask, 0, len(tasks))

	for _, task := range tasks {
		config := task.(map[string]interface{})

		mappedTask := OrchestrationTask{
			Component:         config["component"].(string),
			Action:            config["action"].(string),
			ActionParameters:  config["actionParameters"].(map[string]interface{}),
			Timeout:           config["timeout"].(int),
			IsActive:          config["isActive"].(bool),
			ContinueOnFailure: config["continueOnFailure"].(bool),
			Phase:             config["phase"].(string),
		}

		mappedTasks = append(mappedTasks, mappedTask)
	}

	tasksJSON, err := json.Marshal(mappedTasks)

	if err != nil {
		return err
	}

	client := meta.(*KbcClient)

	tasksBuffer := bytes.NewBuffer(tasksJSON)
	putResp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", orchestrationID), tasksBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	d.SetId(orchestrationID)

	return resourceKeboolaOrchestrationTasksRead(d, meta)
}

func resourceKeboolaOrchestrationTasksRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Orchestration Tasks from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KbcClient)

	getResp, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", d.Id()))

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var orchestrationTasks []OrchestrationTask

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&orchestrationTasks)

	if err != nil {
		return err
	}

	var tasks []map[string]interface{}

	for _, orchestrationTask := range orchestrationTasks {
		taskDetails := map[string]interface{}{
			"component":         orchestrationTask.Component,
			"action":            orchestrationTask.Action,
			"actionParameters":  orchestrationTask.ActionParameters,
			"timeout":           orchestrationTask.Timeout,
			"isActive":          orchestrationTask.IsActive,
			"continueOnFailure": orchestrationTask.ContinueOnFailure,
			"phase":             orchestrationTask.Phase,
		}

		tasks = append(tasks, taskDetails)
	}

	d.Set("orchestration_id", d.Id())
	d.Set("task", tasks)

	return nil
}

func resourceKeboolaOrchestrationTasksUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating OrchestrationTask in Keboola.")

	orchestrationID := d.Get("orchestration_id").(string)
	tasks := d.Get("task").([]interface{})
	mappedTasks := make([]OrchestrationTask, 0, len(tasks))

	for _, task := range tasks {
		config := task.(map[string]interface{})

		mappedTask := OrchestrationTask{
			Component:         config["component"].(string),
			Action:            config["action"].(string),
			ActionParameters:  config["actionParameters"].(map[string]interface{}),
			Timeout:           config["timeout"].(int),
			IsActive:          config["isActive"].(bool),
			ContinueOnFailure: config["continueOnFailure"].(bool),
			Phase:             config["phase"].(string),
		}

		mappedTasks = append(mappedTasks, mappedTask)
	}

	tasksJSON, err := json.Marshal(mappedTasks)

	if err != nil {
		return err
	}

	client := meta.(*KbcClient)

	tasksBuffer := bytes.NewBuffer(tasksJSON)
	putResp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", orchestrationID), tasksBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	d.SetId(orchestrationID)

	return resourceKeboolaOrchestrationTasksRead(d, meta)
}

func resourceKeboolaOrchestrationTasksDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting OrchestrationTask in Keboola: %s", d.Id())

	orchestrationID := d.Get("orchestration_id").(string)

	client := meta.(*KbcClient)
	tasksBuffer := bytes.NewBufferString("[]")
	putResp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", orchestrationID), tasksBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	d.SetId("")

	return nil
}
