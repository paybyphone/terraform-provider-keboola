package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaCSVImportExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaCSVImportExtractorCreate,
		Read:   resourceKeboolaCSVImportExtractorRead,
		Update: resourceKeboolaCSVImportExtractorUpdate,
		Delete: resourceKeboolaCSVImportExtractorDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"destination": {
				Type:     schema.TypeString,
				Required: true,
			},
			"incremental": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"delimiter": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  ",",
			},
			"enclosure": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "\"",
			},
			"primary_key": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}
