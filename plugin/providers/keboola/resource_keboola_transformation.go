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

	config := Configuration{
		Name:        d.Get("name").(string),
		Description: d.Get("description").(string),
		BackEnd:     d.Get("backend").(string),
		Type:        d.Get("type").(string),
		Disabled:    d.Get("disabled").(bool),
		Phase:       KBCNumberString(d.Get("phase").(string)),
	}

	if q := d.Get("queries"); q != nil {
		config.Queries = AsStringArray(q.([]interface{}))
	}

	config.Input = mapInputSchemaToModel(d.Get("input").([]interface{}))
	config.Output = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	configJSON, err := json.Marshal(config)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(configJSON))

	formData := buffer.FromForm(form)

	client := meta.(*KBCClient)
	resp, err := client.PostToStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows", bucketID), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var transform CreateResourceResult

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&transform)

	if err != nil {
		return err
	}

	d.SetId(string(transform.ID))

	return resourceKeboolaTransformRead(d, meta)
}

func resourceKeboolaTransformRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Transformations from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows", d.Get("bucket_id")))

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

	var transform []Transformation

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&transform)

	if err != nil {
		return err
	}

	for _, row := range transform {
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

	config := Configuration{
		Name:        d.Get("name").(string),
		Description: d.Get("description").(string),
		BackEnd:     d.Get("backend").(string),
		Type:        d.Get("type").(string),
		Disabled:    d.Get("disabled").(bool),
		Phase:       KBCNumberString(d.Get("phase").(string)),
	}

	if q := d.Get("queries"); q != nil {
		config.Queries = AsStringArray(q.([]interface{}))
	}

	config.Input = mapInputSchemaToModel(d.Get("input").([]interface{}))
	config.Output = mapOutputSchemaToModel(d.Get("output").([]interface{}))

	configJSON, err := json.Marshal(config)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(configJSON))

	formData := buffer.FromForm(form)

	client := meta.(*KBCClient)
	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", bucketID, d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaTransformRead(d, meta)
}

func resourceKeboolaTransformDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Transformation in Keboola: %s", d.Id())

	bucketID := d.Get("bucket_id").(string)

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", bucketID, d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
