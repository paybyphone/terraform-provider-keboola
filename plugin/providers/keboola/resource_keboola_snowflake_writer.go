package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

type SnowflakeWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   string `json:"nullable"`
	DefaultValue string `json:"default"`
}

type SnowflakeWriterTable struct {
	DatabaseName string                     `json:"dbName"`
	Export       bool                       `json:"export"`
	TableID      string                     `json:"tableId"`
	Items        []SnowflakeWriterTableItem `json:"items"`
}

type SnowflakeWriterDatabaseParameters struct {
	Host     string `json:"host"`
	Database string `json:"database"`
	Password string `json:"password"`
	User     string `string:"user"`
	Schema   string `string:"schema"`
	Port     string `string:"port"`
	Driver   string `string:"driver"`
}

type SnowflakeWriterParameters struct {
	Database SnowflakeWriterDatabaseParameters `json:"db"`
	Tables   []SnowflakeWriterTable            `json:"tables"`
}

type SnowflakeWriterConfiguration struct {
	Parameters SnowflakeWriterParameters `json:"parameters"`
}

//SnowflakeWriter is the data model for storage buckets within
//the Keboola Storage API.
type SnowflakeWriter struct {
	ID            string                       `json:"id,omitempty"`
	Name          string                       `json:"name"`
	Description   string                       `json:"description"`
	Configuration SnowflakeWriterConfiguration `json:"configuration"`
}

func resourceKeboolaSnowflakeWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeWriterCreate,
		Read:   resourceKeboolaSnowflakeWriterRead,
		Delete: resourceKeboolaSnowflakeWriterDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
		},
	}
}

func resourceKeboolaSnowflakeWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Storage Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("stage", d.Get("stage").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("backend", d.Get("backend").(string))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	postResp, err := client.PostToStorage("storage/buckets", formdataBuffer)

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

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaSnowflakeWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Storage Buckets from Keboola.")
	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var SnowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&SnowflakeWriter)

	if err != nil {
		return err
	}

	d.Set("id", SnowflakeWriter.ID)
	d.Set("name", strings.TrimPrefix(SnowflakeWriter.Name, "c-"))
	d.Set("stage", SnowflakeWriter.Stage)
	d.Set("description", SnowflakeWriter.Description)
	d.Set("backend", SnowflakeWriter.Backend)

	return nil
}

func resourceKeboolaSnowflakeWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Storage Bucket in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
