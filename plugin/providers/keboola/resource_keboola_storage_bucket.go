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
		Delete: resourceKeboolaStorageBucketDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"stage": &schema.Schema{
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateStorageBucketStage,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"backend": &schema.Schema{
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
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

	if hasErrors(err, postResp) {
		return extractError(err, postResp)
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

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
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

func resourceKeboolaStorageBucketDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Storage Bucket in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
