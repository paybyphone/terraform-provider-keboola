package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
)

type CreateGoodDataProject struct {
	WriterID    string `json:"writerId"`
	Description string `json:"description"`
	AuthToken   string `json:"authToken"`
}

type GoodDataWriter struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func resourceKeboolaGoodDataWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataWriterCreate,
		Read:   resourceKeboolaGoodDataWriterRead,
		Update: resourceKeboolaGoodDataWriterUpdate,
		Delete: resourceKeboolaGoodDataWriterDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": &schema.Schema{
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
			"authToken": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "keboola_demo",
			},
		},
	}
}

func resourceKeboolaGoodDataWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating GoodData Writer in Keboola.")

	writerID := d.Get("writer_id").(string)

	createProject := CreateGoodDataProject{
		WriterID:    writerID,
		Description: d.Get("description").(string),
		AuthToken:   d.Get("authToken").(string),
	}

	createJSON, err := json.Marshal(createProject)
	if err != nil {
		return err
	}

	client := meta.(*KbcClient)

	createBuffer := bytes.NewBuffer(createJSON)
	createWriterResp, err := client.PostToSyrup("gooddata-writer/v2", createBuffer)

	if hasErrors(err, createWriterResp) {
		return extractError(err, createWriterResp)
	}

	createWriterStatus := "waiting"
	var createWriterStatusRes StorageJobStatus

	createWriterDecoder := json.NewDecoder(createWriterResp.Body)
	err = createWriterDecoder.Decode(&createWriterStatusRes)

	if err != nil {
		return err
	}

	jobURL, err := url.Parse(createWriterStatusRes.URL)

	if err != nil {
		return err
	}

	for createWriterStatus != "success" && createWriterStatus != "error" {
		jobStatusResp, jobErr := client.GetFromSyrup(strings.TrimLeft(jobURL.Path, "/"))

		if hasErrors(jobErr, jobStatusResp) {
			return extractError(jobErr, jobStatusResp)
		}

		decoder := json.NewDecoder(jobStatusResp.Body)
		err = decoder.Decode(&createWriterStatusRes)

		if err != nil {
			return err
		}

		time.Sleep(250 * time.Millisecond)
		createWriterStatus = createWriterStatusRes.Status
	}

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	createWriterConfigResp, err := client.PutToStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", writerID), formdataBuffer)

	if err != nil {
		return err
	}

	if hasErrors(err, createWriterConfigResp) {
		return extractError(err, createWriterConfigResp)
	}

	var createRes CreateResourceResult

	createDecoder := json.NewDecoder(createWriterConfigResp.Body)
	err = createDecoder.Decode(&createRes)

	if err != nil {
		return err
	}

	d.SetId(string(createRes.ID))

	return resourceKeboolaGoodDataWriterRead(d, meta)
}

func resourceKeboolaGoodDataWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading GoodData Writers from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", d.Id()))

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var goodDataWriter GoodDataWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&goodDataWriter)

	if err != nil {
		return err
	}

	d.Set("id", goodDataWriter.ID)
	d.Set("name", goodDataWriter.Name)
	d.Set("description", goodDataWriter.Description)

	return nil
}

func resourceKeboolaGoodDataWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating GoodData Writer in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	client := meta.(*KbcClient)
	formdataBuffer := bytes.NewBufferString(form.Encode())
	putResp, err := client.PutToStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", d.Id()), formdataBuffer)

	if err != nil {
		return err
	}

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaGoodDataWriterRead(d, meta)
}

func resourceKeboolaGoodDataWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData Writer in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	delFromSyrupResp, err := client.DeleteFromSyrup(fmt.Sprintf("gooddata-writer/configs/%s", d.Id()))

	if hasErrors(err, delFromSyrupResp) {
		return extractError(err, delFromSyrupResp)
	}

	delFromStorageResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", d.Id()))

	if hasErrors(err, delFromStorageResp) {
		return extractError(err, delFromStorageResp)
	}

	d.SetId("")

	return nil
}
