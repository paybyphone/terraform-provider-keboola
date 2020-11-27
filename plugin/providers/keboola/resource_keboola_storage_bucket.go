package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/paybyphone/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

//StorageBucket is the data model for storage buckets within
//the Keboola Storage API.
type StorageBucket struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Stage       string `json:"stage"`
	Description string `json:"description"`
	Backend     string `json:"backend,omitempty"`
}

//endregion

func resourceKeboolaStorageBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaStorageBucketCreate,
		Read:   resourceKeboolaStorageBucketRead,
		Delete: resourceKeboolaStorageBucketDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"stage": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateStorageBucketStage,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"backend": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ValidateFunc: validateStorageBucketBackend,
			},
			"is_linked": {
				Type:         schema.TypeBool,
				Optional:     true,
				ForceNew:     true,
			},
			"source_project_id": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
			},
			"source_bucket_id": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
			},
		},
	}
}

func resourceKeboolaStorageBucketCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Storage Bucket in Keboola.")

	createBucketForm := url.Values{}
	createBucketForm.Add("name", d.Get("name").(string))
	createBucketForm.Add("stage", d.Get("stage").(string))
	createBucketForm.Add("description", d.Get("description").(string))
	createBucketForm.Add("backend", d.Get("backend").(string))

	if d.Get("is_linked").(bool) == true {
		createBucketForm.Add("sourceProjectId", d.Get("source_project_id").(string))
		createBucketForm.Add("sourceBucketId", d.Get("source_bucket_id").(string))
	}

	createBucketBuffer := buffer.FromForm(createBucketForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/buckets", createBucketBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	var createBucketResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createBucketResult)

	if err != nil {
		return err
	}

	d.SetId(string(createBucketResult.ID))

	return resourceKeboolaStorageBucketRead(d, meta)
}

func resourceKeboolaStorageBucketRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Storage Buckets from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var storageBucket StorageBucket

	decoder := json.NewDecoder(getResponse.Body)
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

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
