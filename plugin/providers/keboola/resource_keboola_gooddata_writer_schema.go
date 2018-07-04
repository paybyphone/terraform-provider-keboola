package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaGoodDataWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataWriterCreate,
		Read:   resourceKeboolaGoodDataWriterRead,
		Update: resourceKeboolaGoodDataWriterUpdate,
		Delete: resourceKeboolaGoodDataWriterDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"auth_token": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "keboola_demo",
			},
		},
	}
}
