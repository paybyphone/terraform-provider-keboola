package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaSnowflakeWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeWriterTablesCreate,
		Read:   resourceKeboolaSnowflakeWriterTablesRead,
		Update: resourceKeboolaSnowflakeWriterTablesUpdate,
		Delete: resourceKeboolaSnowflakeWriterTablesDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"table": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"db_name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"export": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"table_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"incremental": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"primary_key": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"column": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"db_name": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"type": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"size": {
										Type:     schema.TypeString,
										Required: true,
									},
									"nullable": {
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
									},
									"default": {
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
