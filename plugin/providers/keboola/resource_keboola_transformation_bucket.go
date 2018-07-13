package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaTransformBucketCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Transformation Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	formData := buffer.FromForm(form)

	client := meta.(*KBCClient)
	resp, err := client.PostToStorage("storage/components/transformation/configs", formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var bucket CreateResourceResult

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&bucket)

	if err != nil {
		return err
	}

	d.SetId(string(bucket.ID))

	return resourceKeboolaTransformBucketRead(d, meta)
}

func resourceKeboolaTransformBucketRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Transformation Buckets from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, resp) {
		if err != nil {
			return err
		}

		if resp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, resp)
	}

	var bucket TransformationBucket

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&bucket)

	if err != nil {
		return err
	}

	d.Set("id", bucket.ID)
	d.Set("name", bucket.Name)
	d.Set("description", bucket.Description)

	return nil
}

func resourceKeboolaTransformBucketUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Transformation Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	formData := buffer.FromForm(form)

	client := meta.(*KBCClient)
	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaTransformBucketRead(d, meta)
}

func resourceKeboolaTransformBucketDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Transformation Bucket in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/transformation/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
