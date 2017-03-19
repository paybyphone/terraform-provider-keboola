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
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

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
			"output": &outputSchema,
			"input":  &inputSchema,
		},
	}
}

func resourceKeboolaGoodDataUserManagementCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating GoodData User Management in Keboola.")

	goodDataUserManagementConfig := GoodDataUserManagementConfiguration{
		Storage: GoodDataUserManagementStorage{
			Input:  GoodDataUserManagementInput{},
			Output: GoodDataUserManagementOutput{},
		},
		Parameters: GoodDataUserManagementParameters{
			Writer: d.Get("writer").(string),
		},
	}

	goodDataUserManagementConfig.Storage.Input.Tables = mapInputSchemaToModel(d.Get("input").([]interface{}))
	goodDataUserManagementConfig.Storage.Output.Tables = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	goodDataUserManagementJSON, err := json.Marshal(goodDataUserManagementConfig)

	if err != nil {
		return err
	}

	createUserManagementForm := url.Values{}
	createUserManagementForm.Add("name", d.Get("name").(string))
	createUserManagementForm.Add("description", d.Get("description").(string))
	createUserManagementForm.Add("configuration", string(goodDataUserManagementJSON))

	createUserManagementBuffer := bytes.NewBufferString(createUserManagementForm.Encode())

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs"), createUserManagementBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	var createUserManagementResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createUserManagementResult)

	if err != nil {
		return err
	}

	d.SetId(string(createUserManagementResult.ID))

	return resourceKeboolaGoodDataUserManagementRead(d, meta)
}

func resourceKeboolaGoodDataUserManagementRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading GoodData User Management settings from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResponse)
	}

	var goodDataUserManagement GoodDataUserManagement

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&goodDataUserManagement)

	if err != nil {
		return err
	}

	inputs := mapInputModelToSchema(goodDataUserManagement.Configuration.Storage.Input.Tables)
	outputs := mapOutputModelToSchema(goodDataUserManagement.Configuration.Storage.Output.Tables)

	d.Set("id", goodDataUserManagement.ID)
	d.Set("name", goodDataUserManagement.Name)
	d.Set("description", goodDataUserManagement.Description)
	d.Set("input", inputs)
	d.Set("output", outputs)

	return nil
}

func resourceKeboolaGoodDataUserManagementUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating GoodData User Management settings in Keboola.")

	goodDataUserManagementConfig := GoodDataUserManagementConfiguration{
		Storage: GoodDataUserManagementStorage{
			Input:  GoodDataUserManagementInput{},
			Output: GoodDataUserManagementOutput{},
		},
		Parameters: GoodDataUserManagementParameters{
			Writer: d.Get("writer").(string),
		},
	}

	goodDataUserManagementConfig.Storage.Input.Tables = mapInputSchemaToModel(d.Get("input").([]interface{}))
	goodDataUserManagementConfig.Storage.Output.Tables = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	goodDataUserManagementJSON, err := json.Marshal(goodDataUserManagementConfig)

	if err != nil {
		return err
	}

	updateUserManagementForm := url.Values{}
	updateUserManagementForm.Add("name", d.Get("name").(string))
	updateUserManagementForm.Add("description", d.Get("description").(string))
	updateUserManagementForm.Add("configuration", string(goodDataUserManagementJSON))

	updateUserManagementBuffer := bytes.NewBufferString(updateUserManagementForm.Encode())

	client := meta.(*KBCClient)
	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()), updateUserManagementBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaGoodDataUserManagementRead(d, meta)
}

func resourceKeboolaGoodDataUserManagementDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData User Management in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
