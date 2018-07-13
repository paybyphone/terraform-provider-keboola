package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaGoodDataUserManagementCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating GoodData User Management in Keboola.")

	gdConfig := GoodDataUserManagementConfiguration{
		Storage: GoodDataUserManagementStorage{
			Input:  GoodDataUserManagementInput{},
			Output: GoodDataUserManagementOutput{},
		},
		Parameters: GoodDataUserManagementParameters{
			Writer: d.Get("writer").(string),
		},
	}

	gdConfig.Storage.Input.Tables = mapInputSchemaToModel(d.Get("input").([]interface{}))
	gdConfig.Storage.Output.Tables = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	jsonData, err := json.Marshal(gdConfig)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("configuration", string(jsonData))

	formBuffer := buffer.FromForm(form)

	client := meta.(*KBCClient)
	resp, err := client.PostToStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs"), formBuffer)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var res CreateResourceResult

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&res)

	if err != nil {
		return err
	}

	d.SetId(string(res.ID))

	return resourceKeboolaGoodDataUserManagementRead(d, meta)
}

func resourceKeboolaGoodDataUserManagementRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading GoodData User Management settings from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, resp) {
		if err != nil {
			return err
		}

		if resp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, resp)
	}

	var gdConfig GoodDataUserManagement

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&gdConfig)

	if err != nil {
		return err
	}

	inputs := mapInputModelToSchema(gdConfig.Configuration.Storage.Input.Tables)
	outputs := mapOutputModelToSchema(gdConfig.Configuration.Storage.Output.Tables)

	d.Set("id", gdConfig.ID)
	d.Set("name", gdConfig.Name)
	d.Set("description", gdConfig.Description)
	d.Set("input", inputs)
	d.Set("output", outputs)

	return nil
}

func resourceKeboolaGoodDataUserManagementUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating GoodData User Management settings in Keboola.")

	gdConfig := GoodDataUserManagementConfiguration{
		Storage: GoodDataUserManagementStorage{
			Input:  GoodDataUserManagementInput{},
			Output: GoodDataUserManagementOutput{},
		},
		Parameters: GoodDataUserManagementParameters{
			Writer: d.Get("writer").(string),
		},
	}

	gdConfig.Storage.Input.Tables = mapInputSchemaToModel(d.Get("input").([]interface{}))
	gdConfig.Storage.Output.Tables = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	jsonData, err := json.Marshal(gdConfig)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("configuration", string(jsonData))

	formData := buffer.FromForm(form)

	client := meta.(*KBCClient)
	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaGoodDataUserManagementRead(d, meta)
}

func resourceKeboolaGoodDataUserManagementDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData User Management in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
