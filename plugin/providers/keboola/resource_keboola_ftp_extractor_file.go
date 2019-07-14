package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

type FTPFile struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Description   string                 `json:"description"`
	Configuration map[string]interface{} `json:"configuration"`
}

func resourceKeboolaFTPExtractorFile() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaFTPExtractorFileCreate,
		Read:   resourceKeboolaFTPExtractorFileRead,
		Update: resourceKeboolaFTPExtractorFileUpdate,
		Delete: resourceKeboolaFTPExtractorFileDelete,

		Schema: map[string]*schema.Schema{
			"extractor_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"configuration": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  defaultFileConfig,
			},
		},
	}
}

func resourceKeboolaFTPExtractorFileCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating FTP Extractor File in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))
	createExtractorForm.Add("configuration", d.Get("configuration").(string))
	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)

	extractorID := d.Get("extractor_id").(string)
	createResponse, err := client.PostToStorage(fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s/rows", extractorID), createExtractorBuffer)

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

	return resourceKeboolaFTPExtractorFileRead(d, meta)
}

func resourceKeboolaFTPExtractorFileRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading FTP Extractor File from Keboola.")

	if d.Id() == "" {
		return nil
	}

	extractorID := d.Get("extractor_id").(string)

	client := meta.(*KBCClient)

	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s/rows/%s", extractorID, d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var ftpFile FTPFile

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&ftpFile)

	if err != nil {
		return err
	}

	d.Set("extractor_id", extractorID)

	d.Set("name", ftpFile.Name)
	d.Set("description", ftpFile.Description)
	d.Set("configuration", ftpFile.Configuration)

	return nil
}

func resourceKeboolaFTPExtractorFileUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating FTP Extractor File in Keboola.")

	client := meta.(*KBCClient)

	extractorID := d.Get("extractor_id").(string)

	updateExtractorForm := url.Values{}
	updateExtractorForm.Add("name", d.Get("name").(string))
	updateExtractorForm.Add("description", d.Get("description").(string))
	updateExtractorForm.Add("configuration", d.Get("configuration").(string))
	updateExtractorBuffer := buffer.FromForm(updateExtractorForm)

	updateExtractorResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s/rows/%s", extractorID, d.Id()), updateExtractorBuffer)

	if hasErrors(err, updateExtractorResponse) {
		return extractError(err, updateExtractorResponse)
	}

	return resourceKeboolaFTPExtractorFileRead(d, meta)
}

func resourceKeboolaFTPExtractorFileDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting FTP Extractor File from Keboola: %s", d.Id())

	extractorID := d.Get("extractor_id").(string)

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s/rows/%s", extractorID, d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}

const defaultFileConfig = `{
	"parameters": {
	  "onlyNewFiles": false,
	  "path": ""
	},
	"processors": {
	  "after": [
		{
		  "definition": {
			"component": "keboola.processor-move-files"
		  },
		  "parameters": {
			"direction": "tables",
			"addCsvSuffix": true,
			"folder": "default"
		  }
		},
		{
		  "definition": {
			"component": "keboola.processor-create-manifest"
		  },
		  "parameters": {
			"delimiter": ",",
			"enclosure": "\"",
			"incremental": false,
			"primary_key": [],
			"columns_from": "header"
		  }
		},
		{
		  "definition": {
			"component": "keboola.processor-skip-lines"
		  },
		  "parameters": {
			"lines": 1
		  }
		}
	  ]
	}
  }`
