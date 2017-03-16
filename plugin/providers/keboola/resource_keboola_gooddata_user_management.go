package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
)

type GoodDataUserManagementParameters struct {
	Writer string `json:"gd_writer"`
}

type GoodDataUserManagementInput struct {
	Tables []Input `json:"tables,omitempty"`
}

type GoodDataUserManagementOutput struct {
	Tables []Output `json:"tables,omitempty"`
}

type GoodDataUserManagementStorage struct {
	Input  GoodDataUserManagementInput  `json:"input"`
	Output GoodDataUserManagementOutput `json:"output"`
}

type GoodDataUserManagementConfiguration struct {
	Storage    GoodDataUserManagementStorage    `json:"storage"`
	Parameters GoodDataUserManagementParameters `json:"parameters"`
}

type GoodDataUserManagement struct {
	ID            string                              `json:"id,omitempty"`
	Name          string                              `json:"name"`
	Description   string                              `json:"description,omitempty"`
	Configuration GoodDataUserManagementConfiguration `json:"configuration"`
}

func resourceKeboolaGoodDataUserManagement() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataUserManagementCreate,
		Read:   resourceKeboolaGoodDataUserManagementRead,
		Update: resourceKeboolaGoodDataUserManagementUpdate,
		Delete: resourceKeboolaGoodDataUserManagementDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"writer": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
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

func resourceKeboolaGoodDataUserManagementCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating GoodData User Management in Keboola.")

	gdUserConf := GoodDataUserManagementConfiguration{
		Storage: GoodDataUserManagementStorage{
			Input:  GoodDataUserManagementInput{},
			Output: GoodDataUserManagementOutput{},
		},
		Parameters: GoodDataUserManagementParameters{
			Writer: d.Get("writer").(string),
		},
	}

	gdUserConf.Storage.Output.Tables = mapOutputs(d, meta)
	gdUserConf.Storage.Input.Tables = mapInputs(d, meta)

	gdUserJSON, err := json.Marshal(gdUserConf)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("configuration", string(gdUserJSON))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	postResp, err := client.PostToStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs"), formdataBuffer)

	if hasErrors(err, postResp) {
		return extractError(err, postResp)
	}

	var createRes CreateResourceResult

	decoder := json.NewDecoder(postResp.Body)
	err = decoder.Decode(&createRes)

	if err != nil {
		return err
	}

	d.SetId(string(createRes.ID))

	return resourceKeboolaGoodDataUserManagementRead(d, meta)
}

func resourceKeboolaGoodDataUserManagementRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading GoodData User Management settings from Keboola.")

	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResp) {
		if getResp.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResp)
	}

	var goodDataUserManagement GoodDataUserManagement

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&goodDataUserManagement)

	if err != nil {
		return err
	}

	var inputs []map[string]interface{}
	var outputs []map[string]interface{}

	for _, input := range goodDataUserManagement.Configuration.Storage.Input.Tables {
		inputDetails := map[string]interface{}{
			"source":        input.Source,
			"destination":   input.Destination,
			"columns":       input.Columns,
			"whereOperator": input.WhereOperator,
			"whereValues":   input.WhereValues,
			"whereColumn":   input.WhereColumn,
		}

		inputs = append(inputs, inputDetails)
	}

	for _, output := range goodDataUserManagement.Configuration.Storage.Output.Tables {
		outputDetails := map[string]interface{}{
			"source":      output.Source,
			"destination": output.Destination,
		}

		outputs = append(outputs, outputDetails)
	}

	d.Set("id", goodDataUserManagement.ID)
	d.Set("name", goodDataUserManagement.Name)
	d.Set("description", goodDataUserManagement.Description)
	d.Set("input", inputs)
	d.Set("output", outputs)

	return nil
}

func resourceKeboolaGoodDataUserManagementUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating GoodData User Management settings in Keboola.")

	gdUserConf := GoodDataUserManagementConfiguration{
		Storage: GoodDataUserManagementStorage{
			Input:  GoodDataUserManagementInput{},
			Output: GoodDataUserManagementOutput{},
		},
		Parameters: GoodDataUserManagementParameters{
			Writer: d.Get("writer").(string),
		},
	}

	gdUserConf.Storage.Input.Tables = mapInputs(d, meta)
	gdUserConf.Storage.Output.Tables = mapOutputs(d, meta)

	gdUserJSON, err := json.Marshal(gdUserConf)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("configuration", string(gdUserJSON))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	putResp, err := client.PutToStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()), formdataBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaGoodDataUserManagementRead(d, meta)
}

func resourceKeboolaGoodDataUserManagementDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData User Management in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
