package keboola

import "github.com/hashicorp/terraform/helper/schema"

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
