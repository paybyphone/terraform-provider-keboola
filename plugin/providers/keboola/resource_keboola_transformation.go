package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaTransformCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Transformation in Keboola.")

	bucketID := d.Get("bucket_id").(string)

	transformConfig := Configuration{
		Name:        d.Get("name").(string),
		Description: d.Get("description").(string),
		BackEnd:     d.Get("backend").(string),
		Type:        d.Get("type").(string),
		Disabled:    d.Get("disabled").(bool),
		Phase:       KBCNumberString(d.Get("phase").(string)),
	}

	if q := d.Get("queries"); q != nil {
		transformConfig.Queries = AsStringArray(q.([]interface{}))
	}

	transformConfig.Input = mapInputSchemaToModel(d.Get("input").([]interface{}))
	transformConfig.Output = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	transformJSON, err := json.Marshal(transformConfig)

	if err != nil {
		return err
	}

	createTransformForm := url.Values{}
	createTransformForm.Add("configuration", string(transformJSON))

	createTransformBuffer := buffer.FromForm(createTransformForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows", bucketID), createTransformBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	var createResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createResult)

	if err != nil {
		return err
	}

	d.SetId(string(createResult.ID))

	return resourceKeboolaTransformRead(d, meta)
}

func resourceKeboolaTransformRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Transformations from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", d.Get("bucket_id"), d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var transformation []Transformation

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&transformation)

	if err != nil {
		return err
	}

	for _, row := range transformation {
		if row.Configuration.ID == d.Id() {
			inputs := mapInputModelToSchema(row.Configuration.Input)
			outputs := mapOutputModelToSchema(row.Configuration.Output)

			d.Set("id", row.Configuration.ID)
			d.Set("name", row.Configuration.Name)
			d.Set("description", row.Configuration.Description)
			d.Set("queries", row.Configuration.Queries)
			d.Set("backend", row.Configuration.BackEnd)
			d.Set("disabled", row.Configuration.Disabled)
			d.Set("phase", row.Configuration.Phase)
			d.Set("type", row.Configuration.Type)
			d.Set("output", outputs)
			d.Set("input", inputs)
		}
	}

	return nil
}

func resourceKeboolaTransformUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Transformation in Keboola.")

	bucketID := d.Get("bucket_id").(string)

	transformConfig := Configuration{
		Name:        d.Get("name").(string),
		Description: d.Get("description").(string),
		BackEnd:     d.Get("backend").(string),
		Type:        d.Get("type").(string),
		Disabled:    d.Get("disabled").(bool),
		Phase:       KBCNumberString(d.Get("phase").(string)),
	}

	if q := d.Get("queries"); q != nil {
		transformConfig.Queries = AsStringArray(q.([]interface{}))
	}

	transformConfig.Input = mapInputSchemaToModel(d.Get("input").([]interface{}))
	transformConfig.Output = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	transformJSON, err := json.Marshal(transformConfig)

	if err != nil {
		return err
	}

	updateTransformForm := url.Values{}
	updateTransformForm.Add("configuration", string(transformJSON))

	updateTransformBuffer := buffer.FromForm(updateTransformForm)

	client := meta.(*KBCClient)
	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", bucketID, d.Id()), updateTransformBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaTransformRead(d, meta)
}

func resourceKeboolaTransformDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Transformation in Keboola: %s", d.Id())

	bucketID := d.Get("bucket_id").(string)

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", bucketID, d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
