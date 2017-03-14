package keboola

import (
	"log"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceKeboolaSnowflakeWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeWriterTablesCreate,
		Read:   resourceKeboolaSnowflakeWriterTablesRead,
		Update: resourceKeboolaSnowflakeWriterTablesUpdate,
		Delete: resourceKeboolaSnowflakeWriterTablesDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"table": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"dbName": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"export": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"tableId": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"source": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"destination": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"table": &schema.Schema{
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"dbName": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"type": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"size": &schema.Schema{
										Type:     schema.TypeString,
										Required: true,
									},
									"nullable": &schema.Schema{
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
									},
									"default": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
										Default:  "",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceKeboolaSnowflakeWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Orchestration Tasks in Keboola.")

	//writerID := d.Get("writer_id").(string)
	// tasks := d.Get("task").([]interface{})
	// mappedTasks := make([]OrchestrationTask, 0, len(tasks))
	//
	// for _, task := range tasks {
	// 	config := task.(map[string]interface{})
	//
	// 	mappedTask := OrchestrationTask{
	// 		Component:         config["component"].(string),
	// 		Action:            config["action"].(string),
	// 		ActionParameters:  config["actionParameters"].(map[string]interface{}),
	// 		Timeout:           config["timeout"].(int),
	// 		IsActive:          config["isActive"].(bool),
	// 		ContinueOnFailure: config["continueOnFailure"].(bool),
	// 		Phase:             config["phase"].(string),
	// 	}
	//
	// 	mappedTasks = append(mappedTasks, mappedTask)
	// }
	//
	// tasksJSON, err := json.Marshal(mappedTasks)
	//
	// if err != nil {
	// 	return err
	// }
	//
	// client := meta.(*KbcClient)
	//
	// tasksBuffer := bytes.NewBuffer(tasksJSON)
	// putResp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", orchestrationID), tasksBuffer)
	//
	// if hasErrors(err, putResp) {
	// 	return extractError(err, putResp)
	// }
	//
	// d.SetId(orchestrationID)

	return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

func resourceKeboolaSnowflakeWriterTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Orchestration Tasks from Keboola.")
	//
	// if d.Id() == "" {
	// 	return nil
	// }
	//
	// orchestrationID := d.Id()
	//
	// client := meta.(*KbcClient)
	//
	// getResp, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", d.Id()))
	//
	// if hasErrors(err, getResp) {
	// 	return extractError(err, getResp)
	// }
	//
	// var SnowflakeWriterTables []OrchestrationTask
	//
	// decoder := json.NewDecoder(getResp.Body)
	// err = decoder.Decode(&SnowflakeWriterTables)
	//
	// if err != nil {
	// 	return err
	// }
	//
	// var tasks []map[string]interface{}
	//
	// for _, orchestrationTask := range SnowflakeWriterTables {
	// 	taskDetails := map[string]interface{}{
	// 		"component":         orchestrationTask.Component,
	// 		"action":            orchestrationTask.Action,
	// 		"actionParameters":  orchestrationTask.ActionParameters,
	// 		"timeout":           orchestrationTask.Timeout,
	// 		"isActive":          orchestrationTask.IsActive,
	// 		"continueOnFailure": orchestrationTask.ContinueOnFailure,
	// 		"phase":             orchestrationTask.Phase,
	// 	}
	//
	// 	tasks = append(tasks, taskDetails)
	// }
	//
	// d.Set("orchestration_id", orchestrationID)
	// d.Set("task", tasks)

	return nil
}

func resourceKeboolaSnowflakeWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating Orchestration Tasks in Keboola.")

	// orchestrationID := d.Get("orchestration_id").(string)
	// tasks := d.Get("task").([]interface{})
	// mappedTasks := make([]OrchestrationTask, 0, len(tasks))
	//
	// for _, task := range tasks {
	// 	config := task.(map[string]interface{})
	//
	// 	mappedTask := OrchestrationTask{
	// 		Component:         config["component"].(string),
	// 		Action:            config["action"].(string),
	// 		ActionParameters:  config["actionParameters"].(map[string]interface{}),
	// 		Timeout:           config["timeout"].(int),
	// 		IsActive:          config["isActive"].(bool),
	// 		ContinueOnFailure: config["continueOnFailure"].(bool),
	// 		Phase:             config["phase"].(string),
	// 	}
	//
	// 	mappedTasks = append(mappedTasks, mappedTask)
	// }
	//
	// tasksJSON, err := json.Marshal(mappedTasks)
	//
	// if err != nil {
	// 	return err
	// }
	//
	// client := meta.(*KbcClient)
	//
	// tasksBuffer := bytes.NewBuffer(tasksJSON)
	// putResp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", orchestrationID), tasksBuffer)
	//
	// if hasErrors(err, putResp) {
	// 	return extractError(err, putResp)
	// }

	return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

func resourceKeboolaSnowflakeWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Orchestration Tasks in Keboola: %s", d.Id())
	//
	// client := meta.(*KbcClient)
	// tasksBuffer := bytes.NewBufferString("[]")
	// putResp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s/tasks", d.Id()), tasksBuffer)
	//
	// if hasErrors(err, putResp) {
	// 	return extractError(err, putResp)
	// }
	//
	// d.SetId("")

	return nil
}
