package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
)

//TransformationBucket is the data model for data transformations within
//the Keboola Storage API.
type TransformationBucket struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description"`
}

func resourceKeboolaTransformationBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTransformBucketCreate,
		Read:   resourceKeboolaTransformBucketRead,
		Update: resourceKeboolaTransformBucketUpdate,
		Delete: resourceKeboolaTransformBucketDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceKeboolaTransformBucketCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Transformation Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	postResp, err := client.PostToStorage("storage/components/transformation/configs", formdataBuffer)

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

	return resourceKeboolaTransformBucketRead(d, meta)
}

func resourceKeboolaTransformBucketRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Transformation Buckets from Keboola.")
	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResp) {
		if getResp.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResp)
	}

	var transBucket TransformationBucket

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&transBucket)

	if err != nil {
		return err
	}

	d.Set("id", transBucket.ID)
	d.Set("name", transBucket.Name)
	d.Set("description", transBucket.Description)

	return nil
}

func resourceKeboolaTransformBucketUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating Transformation Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	putResp, err := client.PutToStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()), formdataBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaTransformBucketRead(d, meta)
}

func resourceKeboolaTransformBucketDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Transformation Bucket in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
