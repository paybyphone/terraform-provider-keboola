package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

type CSVImportExtractor struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	Configuration CSVUploadSettings `json:"configuration"`
}

type CSVUploadSettings struct {
	Destination string   `json:"destination"`
	Incremental bool     `json:"incremental"`
	PrimaryKey  []string `json:"primaryKey"`
	Delimiter   string   `json:"delimiter"`
	Enclosure   string   `json:"enclosure"`
}

//endregion

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

func resourceKeboolaCSVImportExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating CSV Import Extractor in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))

	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.csv-import/configs", createExtractorBuffer)

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

	return resourceKeboolaCSVImportExtractorUpdate(d, meta)
}

func resourceKeboolaCSVImportExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading CSV Import Extractor from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.csv-import/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResponse)
	}

	var csvImportExtractor CSVImportExtractor

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&csvImportExtractor)

	if err != nil {
		return err
	}

	d.Set("id", csvImportExtractor.ID)
	d.Set("name", csvImportExtractor.Name)

	d.Set("description", csvImportExtractor.Description)
	d.Set("destination", csvImportExtractor.Configuration.Destination)
	d.Set("incremental", csvImportExtractor.Configuration.Incremental)
	d.Set("primary_key", csvImportExtractor.Configuration.PrimaryKey)
	d.Set("delimiter", csvImportExtractor.Configuration.Delimiter)
	d.Set("enclosure", csvImportExtractor.Configuration.Enclosure)

	return nil
}

func resourceKeboolaCSVImportExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating CSV Import Extractor in Keboola.")

	client := meta.(*KBCClient)

	updateExtractorForm := url.Values{}
	updateExtractorForm.Add("name", d.Get("name").(string))
	updateExtractorForm.Add("description", d.Get("description").(string))

	uploadSettings := CSVUploadSettings{
		Destination: d.Get("destination").(string),
		Incremental: d.Get("incremental").(bool),
		Delimiter:   d.Get("delimiter").(string),
		Enclosure:   d.Get("enclosure").(string),
		PrimaryKey:  AsStringArray(d.Get("primary_key").([]interface{})),
	}

	uploadSettingsJSON, err := json.Marshal(uploadSettings)

	if err != nil {buffer.FromForm(updateExtractorForm)
		return err
	}

	updateExtractorForm.Add("configuration", string(uploadSettingsJSON))
	updateExtractorBuffer := buffer.FromForm(updateExtractorForm)

	updateExtractorResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.csv-import/configs/%s", d.Id()), updateExtractorBuffer)

	if hasErrors(err, updateExtractorResponse) {
		return extractError(err, updateExtractorResponse)
	}

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaCSVImportExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting CSV Import Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.csv-import/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
