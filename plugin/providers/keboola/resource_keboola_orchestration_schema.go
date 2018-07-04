package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaOrchestration() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaOrchestrationCreate,
		Read:   resourceKeboolaOrchestrationRead,
		Update: resourceKeboolaOrchestrationUpdate,
		Delete: resourceKeboolaOrchestrationDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"schedule_cron": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"notification": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"email": {
							Type:     schema.TypeString,
							Required: true,
						},
						"channel": {
							Type:         schema.TypeString,
							ValidateFunc: validateOrchestrationNotificationChannel,
							Required:     true,
						},
						"parameters": {
							Type:     schema.TypeMap,
							Optional: true,
						},
					},
				},
			},
		},
	}
}
