package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
)

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
		if err != nil {
			return err
		}

		if getResponse.StatusCode == 404 {
			d.SetId("")
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
