package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaAccessTokenCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Access Token in Keboola.")

	client := meta.(*KBCClient)

	q := buildAccessTokenQueryString(d)

	resp, err := client.PostToStorage(fmt.Sprintf("storage/tokens/?%s", q.String()), buffer.Empty())

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var token CreateResourceResult

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&token)

	if err != nil {
		return err
	}

	d.SetId(string(token.ID))

	log.Println(fmt.Sprintf("[INFO] Access Token created in Keboola (ID: %s).", string(token.ID)))

	return resourceKeboolaAccessTokenRead(d, meta)
}

func buildAccessTokenQueryString(d *schema.ResourceData) bytes.Buffer {
	var q bytes.Buffer

	q.WriteString(fmt.Sprintf("description=%s", url.QueryEscape(d.Get("description").(string))))
	q.WriteString(fmt.Sprintf("&canManageBuckets=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_manage_buckets").(bool)))))
	q.WriteString(fmt.Sprintf("&canManageTokens=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_manage_tokens").(bool)))))
	q.WriteString(fmt.Sprintf("&canReadAllFileUploads=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_read_all_file_uploads").(bool)))))
	q.WriteString(fmt.Sprintf("&expiresIn=%s", url.QueryEscape(strconv.Itoa(d.Get("expires_in").(int)))))

	for key, value := range AsStringArray(d.Get("component_access").([]interface{})) {
		q.WriteString(fmt.Sprintf("&componentAccess%%5B%v%%5D=%s", key, value))
	}

	for key, value := range d.Get("bucket_permissions").(map[string]interface{}) {
		q.WriteString(fmt.Sprintf("&bucketPermissions%%5B%s%%5D=%s", key, value))
	}

	return q
}

func resourceKeboolaAccessTokenRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Access Token from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

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

	var token AccessToken

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&token)

	if err != nil {
		return err
	}

	expiryTime := token.ExpiresIn
	createdTime := token.CreatedAt

	// TODO: Should consider changing this to an 'expires_at' field, so that it doesn't change on subsequent reads of the resource
	remaining := expiryTime.Sub(createdTime.UTC())

	d.Set("id", token.ID)
	d.Set("description", token.Description)
	d.Set("can_manage_buckets", token.CanManageBuckets)
	d.Set("can_manage_tokens", token.CanManageTokens)
	d.Set("can_read_all_file_uploads", token.CanReadAllFileUploads)
	d.Set("expires_in", remaining/time.Second)
	d.Set("component_access", token.ComponentAccess)
	d.Set("bucket_permissions", token.BucketPermissions)

	return nil
}

func resourceKeboolaAccessTokenUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Access Token in Keboola.")

	client := meta.(*KBCClient)

	q := buildAccessTokenQueryString(d)

	resp, err := client.PutToStorage(fmt.Sprintf("storage/tokens/%s?%s", d.Id(), url.QueryEscape(q.String())), buffer.Empty())

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	log.Println("[INFO] Access Token in Keboola updated.")

	return resourceKeboolaAccessTokenRead(d, meta)
}

func resourceKeboolaAccessTokenDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Access Token in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	log.Println("[INFO] Access Token in Keboola deleted.")

	return nil
}
