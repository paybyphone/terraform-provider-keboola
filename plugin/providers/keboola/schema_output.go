package keboola

import "github.com/hashicorp/terraform/helper/schema"

var outputSchema = schema.Schema{
	Type:     schema.TypeList,
	Optional: true,
	Elem: &schema.Resource{
		Schema: map[string]*schema.Schema{
			"source": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"destination": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"deleteWhereColumn": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"deleteWhereValues": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"deleteWhereOperator": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"primaryKey": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"incremental": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
			},
		},
	},
}

func mapOutputSchemaToModel(d *schema.ResourceData, meta interface{}) []Output {
	outputs := d.Get("output").([]interface{})
	mappedOutputs := make([]Output, 0, len(outputs))

	for _, outputConfig := range outputs {
		config := outputConfig.(map[string]interface{})

		mappedOutput := Output{
			Source:              config["source"].(string),
			Destination:         config["destination"].(string),
			Incremental:         config["incremental"].(bool),
			DeleteWhereOperator: config["deleteWhereOperator"].(string),
			DeleteWhereColumn:   config["deleteWhereColumn"].(string),
		}

		if q := config["primaryKey"]; q != nil {
			mappedOutput.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		if q := config["deleteWhereValues"]; q != nil {
			mappedOutput.DeleteWhereValues = AsStringArray(q.([]interface{}))
		}

		mappedOutputs = append(mappedOutputs, mappedOutput)
	}

	return mappedOutputs
}

func mapOutputModelToSchema(outputs []Output) []map[string]interface{} {
	var mappedOutputs []map[string]interface{}

	for _, output := range outputs {
		mappedOutput := map[string]interface{}{
			"source":              output.Source,
			"destination":         output.Destination,
			"incremental":         output.Incremental,
			"primaryKey":          output.PrimaryKey,
			"deleteWhereOperator": output.DeleteWhereOperator,
			"deleteWhereValues":   output.DeleteWhereValues,
			"deleteWhereColumn":   output.DeleteWhereColumn,
		}

		mappedOutputs = append(mappedOutputs, mappedOutput)
	}
	return mappedOutputs
}
