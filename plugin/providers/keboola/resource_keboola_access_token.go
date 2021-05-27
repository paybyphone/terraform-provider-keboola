package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/paybyphone/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

type AccessToken struct {
	ID                    string                 `json:"id,omitempty"`
	Description           string                 `json:"description"`
	CreatedAt             KBCTime                `json:"created"`
	CanManageBuckets      bool                   `json:"canManageBuckets"`
	CanManageTokens       bool                   `json:"canManageTokens"`
	CanReadAllFileUploads bool                   `json:"canReadAllFileUploads"`
	ExpiresIn             KBCTime                `json:"expires"`
	ComponentAccess       []string               `json:"componentAccess"`
	BucketPermissions     map[string]interface{} `json:"bucketPermissions"`
}

//endregion

func resourceKeboolaAccessToken() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAccessTokenCreate,
		Read:   resourceKeboolaAccessTokenRead,
		Update: resourceKeboolaAccessTokenUpdate,
		Delete: resourceKeboolaAccessTokenDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"description": {
				Type:     schema.TypeString,
				Required: true,
			},
			"can_manage_buckets": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"can_manage_tokens": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"can_read_all_file_uploads": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"expires_in": {
				Type:     schema.TypeInt,
				Optional: true,
				ForceNew: true,
				Default:  nil,
			},
			"component_access": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"bucket_permissions": {
				Type:         schema.TypeMap,
				Optional:     true,
				ValidateFunc: validateAccessTokenBucketPermissions,
			},
		},
	}
}

func resourceKeboolaAccessTokenCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Access Token in Keboola.")

	var createAccessTokenQueryString bytes.Buffer

	createAccessTokenQueryString.WriteString(fmt.Sprintf("description=%s", url.QueryEscape(d.Get("description").(string))))
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&canManageBuckets=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_manage_buckets").(bool)))))
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&canManageTokens=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_manage_tokens").(bool)))))
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&canReadAllFileUploads=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_read_all_file_uploads").(bool)))))
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&expiresIn=%s", url.QueryEscape(strconv.Itoa(d.Get("expires_in").(int)))))

	for key, value := range AsStringArray(d.Get("component_access").([]interface{})) {
		createAccessTokenQueryString.WriteString(fmt.Sprintf("&componentAccess%%5B%v%%5D=%s", key, value))
	}

	for key, value := range d.Get("bucket_permissions").(map[string]interface{}) {
		createAccessTokenQueryString.WriteString(fmt.Sprintf("&bucketPermissions%%5B%s%%5D=%s", key, value))
	}

	client := meta.(*KBCClient)

	createAccessTokenResponse, err := client.PostToStorage(fmt.Sprintf("storage/tokens/?%s", createAccessTokenQueryString.String()), buffer.Empty())

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}

	var createAccessTokenResult CreateResourceResult

	decoder := json.NewDecoder(createAccessTokenResponse.Body)
	err = decoder.Decode(&createAccessTokenResult)

	if err != nil {
		return err
	}

	d.SetId(string(createAccessTokenResult.ID))

	log.Println(fmt.Sprintf("[INFO] Access Token created in Keboola (ID: %s).", string(createAccessTokenResult.ID)))

	return resourceKeboolaAccessTokenRead(d, meta)
}

func resourceKeboolaAccessTokenRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Access Token from Keboola.")

	client := meta.(*KBCClient)
	getAccessTokenResponse, err := client.GetFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getAccessTokenResponse) {
		if getAccessTokenResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getAccessTokenResponse)
	}

	var accessToken AccessToken

	decoder := json.NewDecoder(getAccessTokenResponse.Body)
	err = decoder.Decode(&accessToken)

	if err != nil {
		return err
	}

	expiryTime := accessToken.ExpiresIn
	createdTime := accessToken.CreatedAt

	remaining := expiryTime.Sub(createdTime.UTC())

	d.Set("id", accessToken.ID)
	d.Set("description", accessToken.Description)
	d.Set("can_manage_buckets", accessToken.CanManageBuckets)
	d.Set("can_manage_tokens", accessToken.CanManageTokens)
	d.Set("can_read_all_file_uploads", accessToken.CanReadAllFileUploads)
	d.Set("expires_in", remaining/time.Second)
	d.Set("component_access", accessToken.ComponentAccess)
	d.Set("bucket_permissions", accessToken.BucketPermissions)

	return nil
}

func resourceKeboolaAccessTokenUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Access Token in Keboola.")

	var updateAccessTokenQueryString bytes.Buffer

	updateAccessTokenQueryString.WriteString(fmt.Sprintf("description=%s", d.Get("description").(string)))
	updateAccessTokenQueryString.WriteString(fmt.Sprintf("canManageBuckets=%v", d.Get("can_manage_buckets").(bool)))
	updateAccessTokenQueryString.WriteString(fmt.Sprintf("canManageTokens=%v", d.Get("can_manage_tokens").(bool)))
	updateAccessTokenQueryString.WriteString(fmt.Sprintf("canReadAllFileUploads=%v", d.Get("can_read_all_file_uploads").(bool)))
	updateAccessTokenQueryString.WriteString(fmt.Sprintf("expiresIn=%v", d.Get("expires_in").(int)))

	for key, value := range AsStringArray(d.Get("component_access").([]interface{})) {
		updateAccessTokenQueryString.WriteString(fmt.Sprintf("componentAccess[%v]=%s", key, value))
	}

	for key, value := range d.Get("bucket_permissions").(map[string]interface{}) {
		updateAccessTokenQueryString.WriteString(fmt.Sprintf("bucketPermissions[%s]=%s", key, value))
	}

	client := meta.(*KBCClient)

	updateAccessTokenResponse, err := client.PutToStorage(fmt.Sprintf("storage/tokens/%s?%s", d.Id(), url.QueryEscape(updateAccessTokenQueryString.String())), buffer.Empty())

	if hasErrors(err, updateAccessTokenResponse) {
		return extractError(err, updateAccessTokenResponse)
	}

	log.Println("[INFO] Access Token in Keboola updated.")

	return resourceKeboolaAccessTokenRead(d, meta)
}

func resourceKeboolaAccessTokenDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Access Token in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyAccessTokenResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

	if hasErrors(err, destroyAccessTokenResponse) {
		return extractError(err, destroyAccessTokenResponse)
	}

	d.SetId("")

	log.Println("[INFO] Access Token in Keboola deleted.")

	return nil
}
