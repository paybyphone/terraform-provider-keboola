package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaCSVImportExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating CSV Import Extractor in Keboola.")

	vals := url.Values{}
	vals.Add("name", d.Get("name").(string))
	vals.Add("description", d.Get("description").(string))

	settings := CSVUploadSettings{
		Destination: d.Get("destination").(string),
		Incremental: d.Get("incremental").(bool),
		Delimiter:   d.Get("delimiter").(string),
		Enclosure:   d.Get("enclosure").(string),
		PrimaryKey:  AsStringArray(d.Get("primary_key").([]interface{})),
	}

	settingsJSON, err := json.Marshal(settings)

	if err != nil {
		return err
	}

	vals.Add("configuration", string(settingsJSON))
	body := buffer.FromForm(vals)

	client := meta.(*KBCClient)
	resp, err := client.PostToStorage("storage/components/keboola.csv-import/configs", body)

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

	return resourceKeboolaCSVImportExtractorRead(d, meta)
}

func resourceKeboolaCSVImportExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading CSV Import Extractor from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.csv-import/configs/%s", d.Id()))

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

	var extractor CSVImportExtractor

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&extractor)

	if err != nil {
		return err
	}

	d.Set("id", extractor.ID)
	d.Set("name", extractor.Name)
	d.Set("description", extractor.Description)

	d.Set("destination", extractor.Configuration.Destination)
	d.Set("incremental", extractor.Configuration.Incremental)
	d.Set("primary_key", extractor.Configuration.PrimaryKey)
	d.Set("delimiter", extractor.Configuration.Delimiter)
	d.Set("enclosure", extractor.Configuration.Enclosure)

	return nil
}

func resourceKeboolaCSVImportExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating CSV Import Extractor in Keboola.")

	client := meta.(*KBCClient)

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	settings := CSVUploadSettings{
		Destination: d.Get("destination").(string),
		Incremental: d.Get("incremental").(bool),
		Delimiter:   d.Get("delimiter").(string),
		Enclosure:   d.Get("enclosure").(string),
		PrimaryKey:  AsStringArray(d.Get("primary_key").([]interface{})),
	}

	settingsJSON, err := json.Marshal(settings)

	if err != nil {
		return err
	}

	form.Add("configuration", string(settingsJSON))
	formData := buffer.FromForm(form)

	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.csv-import/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaCSVImportExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting CSV Import Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.csv-import/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
