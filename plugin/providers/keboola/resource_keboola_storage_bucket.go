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

//StorageBucket is the data model for storage buckets within
//the Keboola Storage API.
type StorageBucket struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Stage       string `json:"stage"`
	Description string `json:"description"`
	Backend     string `json:"backend,omitempty"`
}

func resourceKeboolaStorageBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaStorageBucketCreate,
		Read:   resourceKeboolaStorageBucketRead,
		Update: resourceKeboolaStorageBucketUpdate,
		Delete: resourceKeboolaStorageBucketDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"stage": &schema.Schema{
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validateStorageBucketStage,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"backend": &schema.Schema{
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validateStorageBucketBackend,
			},
		},
	}
}

func resourceKeboolaStorageBucketCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Storage Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("stage", d.Get("stage").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("backend", d.Get("backend").(string))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	postResp, err := client.PostToStorage("storage/buckets", formdataBuffer)

	if err != nil {
		return err
	}

	var createRes CreateResourceResult

	decoder := json.NewDecoder(postResp.Body)
	err = decoder.Decode(&createRes)

	if err != nil {
		return err
	}

	d.SetId(createRes.ID)

	return resourceKeboolaStorageBucketRead(d, meta)
}

func resourceKeboolaStorageBucketRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Storage Buckets from Keboola.")
	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if err != nil {
		return err
	}

	var storageBucket StorageBucket

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&storageBucket)

	if err != nil {
		return err
	}

	d.Set("id", storageBucket.ID)
	d.Set("name", strings.TrimPrefix(storageBucket.Name, "c-"))
	d.Set("stage", storageBucket.Stage)
	d.Set("description", storageBucket.Description)
	d.Set("backend", storageBucket.Backend)

	return nil
}

func resourceKeboolaStorageBucketUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating Storage Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("stage", d.Get("stage").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("backend", d.Get("backend").(string))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	_, err := client.PutToStorage(fmt.Sprintf("storage/buckets/%s", d.Id()), formdataBuffer)

	if err != nil {
		return err
	}

	return resourceKeboolaStorageBucketRead(d, meta)
}

func resourceKeboolaStorageBucketDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Storage Bucket in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	_, err := client.DeleteFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if err != nil {
		return fmt.Errorf("Error deleting Storage Bucket: %s", err)
	}

	d.SetId("")

	return nil
}
