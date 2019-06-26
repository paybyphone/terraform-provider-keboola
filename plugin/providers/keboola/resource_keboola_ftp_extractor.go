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

type FTPExtractor struct {
	ID            string      `json:"id"`
	Name          string      `json:"name"`
	Description   string      `json:"description"`
	Configuration FTPSettings `json:"configuration"`
	Files         []FTPFile   `json:"rows"`
}

type FTPSettings struct {
	Host                string `json:"host"`
	Port                int    `json:"port"`
	ConnectionType      string `json:"connectionType"`
	Username            string `json:"username"`
	EncryptedPassword   string `json:"#password"`
	EncryptedPrivateKey string `json:"#privateKey"`
}

//endregion

func resourceKeboolaFTPExtractor() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaFTPExtractorCreate,
		Read:   resourceKeboolaFTPExtractorRead,
		Update: resourceKeboolaFTPExtractorUpdate,
		Delete: resourceKeboolaFTPExtractorDelete,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"host": {
				Type:     schema.TypeString,
				Required: true,
			},
			"port": {
				Type:     schema.TypeInt,
				Required: true,
			},
			"connection_type": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validateFTPConnectionType,
			},
			"username": {
				Type:     schema.TypeString,
				Required: true,
			},
			"hashed_password": {
				Type:         schema.TypeString,
				Required:     true,
				Sensitive:    true,
				ValidateFunc: validateKBCEncryptedValue,
			},
			"hashed_private_key": {
				Type:         schema.TypeString,
				Optional:     true,
				Sensitive:    true,
				ValidateFunc: validateKBCEncryptedValue,
			},
		},
	}
}

func resourceKeboolaFTPExtractorCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating FTP Extractor in Keboola.")

	createExtractorForm := url.Values{}
	createExtractorForm.Add("name", d.Get("name").(string))
	createExtractorForm.Add("description", d.Get("description").(string))

	uploadSettings := FTPSettings{
		Host:                d.Get("host").(string),
		Port:                d.Get("port").(int),
		ConnectionType:      d.Get("connection_type").(string),
		Username:            d.Get("username").(string),
		EncryptedPassword:   d.Get("hashed_password").(string),
		EncryptedPrivateKey: d.Get("hashed_private_key").(string),
	}

	uploadSettingsJSON, err := json.Marshal(uploadSettings)

	if err != nil {
		return err
	}

	createExtractorForm.Add("configuration", string(uploadSettingsJSON))
	createExtractorBuffer := buffer.FromForm(createExtractorForm)

	client := meta.(*KBCClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.ex-ftp/configs", createExtractorBuffer)

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

	return resourceKeboolaFTPExtractorRead(d, meta)
}

func resourceKeboolaFTPExtractorRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading FTP Extractor from Keboola.")

	client := meta.(*KBCClient)
	getFTPExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getFTPExtractorResponse) {
		if getFTPExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getFTPExtractorResponse)
	}

	var ftpExtractor FTPExtractor

	decoder := json.NewDecoder(getFTPExtractorResponse.Body)
	err = decoder.Decode(&ftpExtractor)

	if err != nil {
		return err
	}

	d.Set("id", ftpExtractor.ID)
	d.Set("name", ftpExtractor.Name)
	d.Set("description", ftpExtractor.Description)

	d.Set("host", ftpExtractor.Configuration.Host)
	d.Set("port", ftpExtractor.Configuration.Port)
	d.Set("connection_type", ftpExtractor.Configuration.ConnectionType)
	d.Set("username", ftpExtractor.Configuration.Username)
	d.Set("hashed_password", ftpExtractor.Configuration.EncryptedPassword)
	d.Set("hashed_private_key", ftpExtractor.Configuration.EncryptedPrivateKey)

	return nil
}

func resourceKeboolaFTPExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating FTP Extractor in Keboola.")

	client := meta.(*KBCClient)

	updateExtractorForm := url.Values{}
	updateExtractorForm.Add("name", d.Get("name").(string))
	updateExtractorForm.Add("description", d.Get("description").(string))

	uploadSettings := FTPSettings{
		Host:                d.Get("host").(string),
		Port:                d.Get("port").(int),
		ConnectionType:      d.Get("connection_type").(string),
		Username:            d.Get("username").(string),
		EncryptedPassword:   d.Get("hashed_password").(string),
		EncryptedPrivateKey: d.Get("hashed_private_key").(string),
	}

	uploadSettingsJSON, err := json.Marshal(uploadSettings)

	if err != nil {
		return err
	}

	updateExtractorForm.Add("configuration", string(uploadSettingsJSON))
	updateExtractorBuffer := buffer.FromForm(updateExtractorForm)

	updateExtractorResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s", d.Id()), updateExtractorBuffer)

	if hasErrors(err, updateExtractorResponse) {
		return extractError(err, updateExtractorResponse)
	}

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaFTPExtractorDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting FTP Extractor in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
