package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
)

type KeboolaTime struct {
	time.Time
}

type AccessToken struct {
	ID                    string                 `json:"id,omitempty"`
	Description           string                 `json:"description"`
	CanManageBuckets      bool                   `json:"canManageBuckets"`
	CanManageTokens       bool                   `json:"canManageTokens"`
	CanReadAllFileUploads bool                   `json:"canReadAllFileUploads"`
	ExpiresIn             KeboolaTime            `json:"expires"`
	ComponentAccess       []string               `json:"componentAccess"`
	BucketPermissions     map[string]interface{} `json:"bucketPermissions"`
}

func (kt *KeboolaTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")

	if s == "null" {
		kt.Time = time.Time{}
		return
	}

	kt.Time, err = time.Parse("2006-01-02T15:04:05-0700", s)

	return
}

func resourceKeboolaAccessToken() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAccessTokenCreate,
		Read:   resourceKeboolaAccessTokenRead,
		Update: resourceKeboolaAccessTokenUpdate,
		Delete: resourceKeboolaAccessTokenDelete,

		Schema: map[string]*schema.Schema{
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"canManageBuckets": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"canManageTokens": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"canReadAllFileUploads": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"expiresIn": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
				ForceNew: true,
				Default:  nil,
			},
			"componentAccess": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"bucketPermissions": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
			},
		},
	}
}

func resourceKeboolaAccessTokenCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Access Token in Keboola.")

	var queryString bytes.Buffer

	queryString.WriteString(fmt.Sprintf("description=%s", url.QueryEscape(d.Get("description").(string))))
	queryString.WriteString(fmt.Sprintf("&canManageBuckets=%s", url.QueryEscape(strconv.FormatBool(d.Get("canManageBuckets").(bool)))))
	queryString.WriteString(fmt.Sprintf("&canManageTokens=%s", url.QueryEscape(strconv.FormatBool(d.Get("canManageTokens").(bool)))))
	queryString.WriteString(fmt.Sprintf("&canReadAllFileUploads=%s", url.QueryEscape(strconv.FormatBool(d.Get("canReadAllFileUploads").(bool)))))
	queryString.WriteString(fmt.Sprintf("&expiresIn=%s", url.QueryEscape(strconv.Itoa(d.Get("expiresIn").(int)))))

	for key, value := range AsStringArray(d.Get("componentAccess").([]interface{})) {
		queryString.WriteString(fmt.Sprintf("&componentAccess%%5B%v%%5D=%s", key, value))
	}

	for key, value := range d.Get("bucketPermissions").(map[string]interface{}) {
		queryString.WriteString(fmt.Sprintf("&bucketPermissions%%5B%s%%5D=%s", key, value))
	}

	client := meta.(*KbcClient)

	emptyBuffer := bytes.NewBufferString("")
	postResp, err := client.PostToStorage(fmt.Sprintf("storage/tokens/?%s", queryString.String()), emptyBuffer)

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

	return resourceKeboolaAccessTokenRead(d, meta)
}

func resourceKeboolaAccessTokenRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Access Tokens from Keboola.")
	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var accessToken AccessToken

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&accessToken)

	if err != nil {
		return err
	}

	expiryTime := accessToken.ExpiresIn
	remaining := expiryTime.Sub(time.Now())

	d.Set("id", accessToken.ID)
	d.Set("description", accessToken.Description)
	d.Set("canManageBuckets", accessToken.CanManageBuckets)
	d.Set("canManageTokens", accessToken.CanManageTokens)
	d.Set("canReadAllFileUploads", accessToken.CanReadAllFileUploads)
	d.Set("expiresIn", remaining/time.Second)
	d.Set("componentAccess", accessToken.ComponentAccess)
	d.Set("bucketPermissions", accessToken.BucketPermissions)

	return nil
}

func resourceKeboolaAccessTokenUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating Access Token in Keboola.")

	var queryString bytes.Buffer

	queryString.WriteString(fmt.Sprintf("description=%s", d.Get("description").(string)))
	queryString.WriteString(fmt.Sprintf("canManageBuckets=%v", d.Get("canManageBuckets").(bool)))
	queryString.WriteString(fmt.Sprintf("canManageTokens=%v", d.Get("canManageTokens").(bool)))
	queryString.WriteString(fmt.Sprintf("canReadAllFileUploads=%v", d.Get("canReadAllFileUploads").(bool)))
	queryString.WriteString(fmt.Sprintf("expiresIn=%v", d.Get("expiresIn").(int)))

	for key, value := range AsStringArray(d.Get("componentAccess").([]interface{})) {
		queryString.WriteString(fmt.Sprintf("componentAccess[%v]=%s", key, value))
	}

	for key, value := range d.Get("bucketPermissions").(map[string]interface{}) {
		queryString.WriteString(fmt.Sprintf("bucketPermissions[%s]=%s", key, value))
	}

	client := meta.(*KbcClient)

	emptyBuffer := bytes.NewBufferString("")
	putResp, err := client.PutToStorage(fmt.Sprintf("storage/tokens/%s?%s", d.Id(), url.QueryEscape(queryString.String())), emptyBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaAccessTokenRead(d, meta)
}

func resourceKeboolaAccessTokenDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Access Token in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/tokens/%s", d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
