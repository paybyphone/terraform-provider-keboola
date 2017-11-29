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
)

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
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"can_manage_buckets": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"can_manage_tokens": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"can_read_all_file_uploads": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"expires_in": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				ForceNew: true,
				Default:  nil,
			},
			"component_access": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"bucket_permissions": &schema.Schema{
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
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&can_manage_buckets=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_manage_buckets").(bool)))))
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&can_manage_tokens=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_manage_tokens").(bool)))))
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&can_read_all_file_uploads=%s", url.QueryEscape(strconv.FormatBool(d.Get("can_read_all_file_uploads").(bool)))))
	createAccessTokenQueryString.WriteString(fmt.Sprintf("&expires_in=%s", url.QueryEscape(strconv.Itoa(d.Get("expires_in").(int)))))

	for key, value := range AsStringArray(d.Get("component_access").([]interface{})) {
		createAccessTokenQueryString.WriteString(fmt.Sprintf("&componentAccess%%5B%v%%5D=%s", key, value))
	}

	for key, value := range d.Get("bucket_permissions").(map[string]interface{}) {
		createAccessTokenQueryString.WriteString(fmt.Sprintf("&bucketPermissions%%5B%s%%5D=%s", key, value))
	}

	client := meta.(*KBCClient)

	//TODO: Have an empty buffer constant or common utility
	emptyBuffer := bytes.NewBufferString("")
	createResponse, err := client.PostToStorage(fmt.Sprintf("storage/tokens/?%s", createAccessTokenQueryString.String()), emptyBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	var createAccessTokenResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createAccessTokenResult)

	if err != nil {
		return err
	}

	d.SetId(string(createAccessTokenResult.ID))

	return resourceKeboolaAccessTokenRead(d, meta)
}

func resourceKeboolaAccessTokenRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Access Tokens from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResponse)
	}

	var accessToken AccessToken

	decoder := json.NewDecoder(getResponse.Body)
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

	emptyBuffer := bytes.NewBufferString("")
	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/tokens/%s?%s", d.Id(), url.QueryEscape(updateAccessTokenQueryString.String())), emptyBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaAccessTokenRead(d, meta)
}

func resourceKeboolaAccessTokenDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Access Token in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
