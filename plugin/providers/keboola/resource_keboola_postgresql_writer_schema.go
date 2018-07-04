package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaPostgreSQLWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaPostgreSQLWriterCreate,
		Read:   resourceKeboolaPostgreSQLWriterRead,
		Update: resourceKeboolaPostgreSQLWriterUpdate,
		Delete: resourceKeboolaPostgreSQLWriterDelete,
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
			"db_parameters": {
				Type:             schema.TypeString,
				Optional:         true,
				Removed: 		  "'db_parameters' has been deprecated, please use 'postgresql_db_parameters' instead",
				DiffSuppressFunc: suppressEquivalentJSON,
			},
			"postgresql_db_parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"hostname": {
							Type:     schema.TypeString,
							Required: true,
						},
						"port": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  5432,
						},
						"database": {
							Type:     schema.TypeString,
							Required: true,
						},
						"schema": {
							Type:     schema.TypeString,
							Required: true,
						},
						"username": {
							Type:     schema.TypeString,
							Required: true,
						},
						"hashed_password": {
							Type:         schema.TypeString,
							Required:     true,
							Sensitive:    true,
							ValidateFunc: validateKBCEncryptedValue,
						},
					},
				},
			},
		},
	}
}
