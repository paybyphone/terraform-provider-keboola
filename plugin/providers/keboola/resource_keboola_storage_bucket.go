package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaStorageBucketCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Storage Bucket in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("stage", d.Get("stage").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("backend", d.Get("backend").(string))

	formData := buffer.FromForm(form)

	client := meta.(*KBCClient)
	resp, err := client.PostToStorage("storage/buckets", formData)

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

	return resourceKeboolaStorageBucketRead(d, meta)
}

func resourceKeboolaStorageBucketRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Storage Buckets from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

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

	var bucket StorageBucket

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&bucket)

	if err != nil {
		return err
	}

	d.Set("id", bucket.ID)
	d.Set("name", strings.TrimPrefix(bucket.Name, "c-"))
	d.Set("stage", bucket.Stage)
	d.Set("description", bucket.Description)
	d.Set("backend", bucket.Backend)

	return nil
}

func resourceKeboolaStorageBucketDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Storage Bucket in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/buckets/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
