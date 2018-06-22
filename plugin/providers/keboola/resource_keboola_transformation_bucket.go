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

//TransformationBucket is the data model for data transformations within
//the Keboola Storage API.
type TransformationBucket struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description"`
}

//endregion

func resourceKeboolaTransformationBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTransformBucketCreate,
		Read:   resourceKeboolaTransformBucketRead,
		Update: resourceKeboolaTransformBucketUpdate,
		Delete: resourceKeboolaTransformBucketDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceKeboolaTransformBucketCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Transformation Bucket in Keboola.")

	createBucketForm := url.Values{}
	createBucketForm.Add("name", d.Get("name").(string))
	createBucketForm.Add("description", d.Get("description").(string))

	createBucketBuffer := buffer.FromForm(createBucketForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/transformation/configs", createBucketBuffer)

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

	return resourceKeboolaTransformBucketRead(d, meta)
}

func resourceKeboolaTransformBucketRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Transformation Buckets from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()))

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

	var transformBucket TransformationBucket

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&transformBucket)

	if err != nil {
		return err
	}

	d.Set("id", transformBucket.ID)
	d.Set("name", transformBucket.Name)
	d.Set("description", transformBucket.Description)

	return nil
}

func resourceKeboolaTransformBucketUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Transformation Bucket in Keboola.")

	updateBucketForm := url.Values{}
	updateBucketForm.Add("name", d.Get("name").(string))
	updateBucketForm.Add("description", d.Get("description").(string))

	updateBucketBuffer := buffer.FromForm(updateBucketForm)

	client := meta.(*KBCClient)
	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()), updateBucketBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaTransformBucketRead(d, meta)
}

func resourceKeboolaTransformBucketDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Transformation Bucket in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
