package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaGoodDataUserManagement() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataUserManagementCreate,
		Read:   resourceKeboolaGoodDataUserManagementRead,
		Update: resourceKeboolaGoodDataUserManagementUpdate,
		Delete: resourceKeboolaGoodDataUserManagementDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"writer": {
				Type:     schema.TypeString,
				Required: true,
			},
			"output": &outputSchema,
			"input":  &inputSchema,
		},
	}
}
