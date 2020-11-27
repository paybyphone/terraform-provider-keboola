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
	"github.com/paybyphone/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

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

//endregion

func resourceKeboolaGoodDataWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataWriterCreate,
		Read:   resourceKeboolaGoodDataWriterRead,
		Update: resourceKeboolaGoodDataWriterUpdate,
		Delete: resourceKeboolaGoodDataWriterDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		DeprecationMessage: "keboola_gooddata_writer has been deprecated and should be replaced with keboola_gooddata_writer_v3",

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"auth_token": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "keboola_demo",
			},
		},
	}
}

func resourceKeboolaGoodDataWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating GoodData Writer in Keboola.")

	writerID := d.Get("writer_id").(string)
	client := meta.(*KBCClient)

	err := provisionGoodDataProject(writerID, d.Get("description").(string), d.Get("auth_token").(string), client)

	if err != nil {
		return err
	}

	createdConfigID, err := createGoodDataWriterConfiguration(writerID, d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetId(createdConfigID)

	return resourceKeboolaGoodDataWriterRead(d, meta)
}

func provisionGoodDataProject(writerID string, description string, authToken string, client *KBCClient) error {
	createProject := CreateGoodDataProject{
		WriterID:    writerID,
		Description: description,
		AuthToken:   authToken,
	}

	createJSON, err := json.Marshal(createProject)
	if err != nil {
		return err
	}

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

	return nil
}

func createGoodDataWriterConfiguration(writerID string, name string, description string, client *KBCClient) (createdID string, err error) {
	form := url.Values{}
	form.Add("name", name)
	form.Add("description", description)

	formdataBuffer := buffer.FromForm(form)

	createWriterConfigResp, err := client.PutToStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", writerID), formdataBuffer)

	if err != nil {
		return "", err
	}

	if hasErrors(err, createWriterConfigResp) {
		return "", extractError(err, createWriterConfigResp)
	}

	var createRes CreateResourceResult

	createDecoder := json.NewDecoder(createWriterConfigResp.Body)
	err = createDecoder.Decode(&createRes)

	if err != nil {
		return "", err
	}

	return string(createRes.ID), nil
}

func resourceKeboolaGoodDataWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading GoodData Writers from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", d.Id()))

	if hasErrors(err, getResp) {
		if getResp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

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
	log.Println("[INFO] Updating GoodData Writer in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	client := meta.(*KBCClient)
	formdataBuffer := buffer.FromForm(form)
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

	client := meta.(*KBCClient)
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
