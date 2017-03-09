package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

//Input is a mapping from input tables to internal tables for
//use by the transformation queries.
type Input struct {
	Source        string                 `json:"source"`
	Destination   string                 `json:"destination"`
	WhereColumn   string                 `json:"whereColumn,omitempty"`
	WhereOperator string                 `json:"whereOperator,omitempty"`
	WhereValues   []string               `json:"whereValues,omitempty"`
	Indexes       [][]string             `json:"indexes,omitempty"`
	Columns       []string               `json:"columns"`
	DataTypes     map[string]interface{} `json:"datatypes"`
	Days          int                    `json:"days,omitempty"`
}

//Output is a mapping from the internal tables used by transformation queries
//to output tables.
type Output struct {
	Source              string   `json:"source"`
	Destination         string   `json:"destination"`
	Incremental         bool     `json:"incremental,omitempty"`
	PrimaryKey          []string `json:"primarykey,omitempty"`
	DeleteWhereValues   []string `json:"deleteWhereValues,omitempty"`
	DeleteWhereOperator string   `json:"deleteWhereOperator,omitempty"`
	DeleteWhereColumn   string   `json:"deleteWhereColumn,omitempty"`
}

//Configuration holds the core configuration for each transformation, as
//it is structured in the Keboola Storage API.
type Configuration struct {
	Input       []Input  `json:"input,omitempty"`
	Output      []Output `json:"output,omitempty"`
	Queries     []string `json:"queries,omitempty"`
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Disabled    bool     `json:"disabled,omitempty"`
	BackEnd     string   `json:"backend"`
	Type        string   `json:"type"`
}

//Transformation is the data model for data transformations within
//the Keboola Storage API.
type Transformation struct {
	ID            string        `json:"id"`
	Configuration Configuration `json:"configuration"`
}

func resourceKeboolaTransformation() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTransformCreate,
		Read:   resourceKeboolaTransformRead,
		Update: resourceKeboolaTransformUpdate,
		Delete: resourceKeboolaTransformDelete,

		Schema: map[string]*schema.Schema{
			"bucket_id": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"backend": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"type": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"disabled": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
			},
			"queries": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"output": &schema.Schema{
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
			},
			"input": &schema.Schema{
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
						"datatypes": &schema.Schema{
							Type:     schema.TypeMap,
							Optional: true,
						},
						"whereColumn": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
							Default:  "",
						},
						"whereValues": &schema.Schema{
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"whereOperator": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
							Default:  "eq",
						},
						"columns": &schema.Schema{
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"indexes": &schema.Schema{
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"days": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func mapInputs(d *schema.ResourceData, meta interface{}) []Input {
	inputs := d.Get("input").([]interface{})
	mappedInputs := make([]Input, 0, len(inputs))

	for _, inputConfig := range inputs {
		config := inputConfig.(map[string]interface{})

		mappedInput := Input{
			Source:        config["source"].(string),
			Destination:   config["destination"].(string),
			WhereOperator: config["whereOperator"].(string),
			WhereColumn:   config["whereColumn"].(string),
			DataTypes:     config["datatypes"].(map[string]interface{}),
			Days:          config["days"].(int),
		}

		if q := config["whereValues"]; q != nil {
			mappedInput.WhereValues = AsStringArray(q.([]interface{}))
		}

		if q := config["columns"]; q != nil {
			mappedInput.Columns = AsStringArray(q.([]interface{}))
		}

		if q := config["indexes"]; q != nil {
			dest := make([][]string, 0, len(q.([]interface{})))
			for _, q := range q.([]interface{}) {
				if q != nil {
					indexes := strings.Split(q.(string), ",")
					dest = append(dest, indexes)
				}
			}
			mappedInput.Indexes = dest
		}

		mappedInputs = append(mappedInputs, mappedInput)
	}

	return mappedInputs
}

func mapOutputs(d *schema.ResourceData, meta interface{}) []Output {
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

		if q := config["primarykey"]; q != nil {
			mappedOutput.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		if q := config["deleteWhereValues"]; q != nil {
			mappedOutput.DeleteWhereValues = AsStringArray(q.([]interface{}))
		}

		mappedOutputs = append(mappedOutputs, mappedOutput)
	}

	return mappedOutputs
}

func resourceKeboolaTransformCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Transformation in Keboola.")

	bucketID := d.Get("bucket_id").(string)

	transConf := Configuration{
		Name:        d.Get("name").(string),
		Description: d.Get("description").(string),
		BackEnd:     d.Get("backend").(string),
		Type:        d.Get("type").(string),
		Disabled:    d.Get("disabled").(bool),
	}

	if q := d.Get("queries"); q != nil {
		transConf.Queries = AsStringArray(q.([]interface{}))
	}

	transConf.Input = mapInputs(d, meta)
	transConf.Output = mapOutputs(d, meta)

	transJSON, err := json.Marshal(transConf)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(transJSON))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	postResp, err := client.PostToStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows", bucketID), formdataBuffer)

	if hasErrors(err, postResp) {
		return extractError(err, postResp)
	}

	var createRes CreateResourceResult

	decoder := json.NewDecoder(postResp.Body)
	err = decoder.Decode(&createRes)

	if err != nil {
		return err
	}

	d.SetId(createRes.ID)

	return resourceKeboolaTransformRead(d, meta)
}

func resourceKeboolaTransformRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Transformations from Keboola.")
	client := meta.(*KbcClient)
	readURI := fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", d.Get("bucket_id"), d.Id())
	getResp, err := client.GetFromStorage(readURI)

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var trans []Transformation

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&trans)

	if err != nil {
		return err
	}

	for _, row := range trans {
		if row.Configuration.ID == d.Id() {
			var inputs []map[string]interface{}
			var outputs []map[string]interface{}

			for _, input := range row.Configuration.Input {
				inputDetails := map[string]interface{}{
					"source":        input.Source,
					"destination":   input.Destination,
					"columns":       input.Columns,
					"whereOperator": input.WhereOperator,
					"whereValues":   input.WhereValues,
					"whereColumn":   input.WhereColumn,
					"datatypes":     input.DataTypes,
					"days":          input.Days,
				}

				if input.Indexes != nil {
					indexDetails := make([]string, 0, len(input.Indexes))

					for _, i := range input.Indexes {
						combinedIndex := strings.Join(i, ",")
						indexDetails = append(indexDetails, combinedIndex)
					}

					inputDetails["indexes"] = indexDetails
				}

				inputs = append(inputs, inputDetails)
			}

			for _, output := range row.Configuration.Output {
				outputDetails := map[string]interface{}{
					"source":              output.Source,
					"destination":         output.Destination,
					"incremental":         output.Incremental,
					"primaryKey":          output.PrimaryKey,
					"deleteWhereOperator": output.DeleteWhereOperator,
					"deleteWhereValues":   output.DeleteWhereValues,
					"deleteWhereColumn":   output.DeleteWhereColumn,
				}

				outputs = append(outputs, outputDetails)
			}

			d.Set("id", row.Configuration.ID)
			d.Set("name", row.Configuration.Name)
			d.Set("description", row.Configuration.Description)
			d.Set("queries", row.Configuration.Queries)
			d.Set("backend", row.Configuration.BackEnd)
			d.Set("disabled", row.Configuration.Disabled)
			d.Set("type", row.Configuration.Type)
			d.Set("output", outputs)
			d.Set("input", inputs)
		}
	}

	return nil
}

func resourceKeboolaTransformUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating Transformation in Keboola.")

	bucketID := d.Get("bucket_id").(string)

	transConf := Configuration{
		Name:        d.Get("name").(string),
		Description: d.Get("description").(string),
		BackEnd:     d.Get("backend").(string),
		Type:        d.Get("type").(string),
		Disabled:    d.Get("disabled").(bool),
	}

	if q := d.Get("queries"); q != nil {
		transConf.Queries = AsStringArray(q.([]interface{}))
	}

	transConf.Input = mapInputs(d, meta)
	transConf.Output = mapOutputs(d, meta)

	transJSON, err := json.Marshal(transConf)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(transJSON))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	putResp, err := client.PutToStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", bucketID, d.Id()), formdataBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaTransformRead(d, meta)
}

func resourceKeboolaTransformDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Transformation in Keboola: %s", d.Id())

	bucketID := d.Get("bucket_id").(string)

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", bucketID, d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
