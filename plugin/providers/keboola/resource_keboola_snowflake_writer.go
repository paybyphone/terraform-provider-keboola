package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"

	"github.com/hashicorp/terraform/helper/schema"
)

type SnowflakeWriterDatabaseParameters struct {
	HostName string `json:"host"`
	Database string `json:"database"`
	Password string `json:"password"`
	Username string `string:"user"`
	Schema   string `string:"schema"`
	Port     string `string:"port"`
	Driver   string `string:"driver"`
}

type SnowflakeWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}

type SnowflakeWriterTable struct {
	DatabaseName string                     `json:"dbName"`
	Export       bool                       `json:"export"`
	Incremental  bool                       `json:"incremental"`
	TableID      string                     `json:"tableId"`
	PrimaryKey   []string                   `json:"primaryKey,omitempty"`
	Items        []SnowflakeWriterTableItem `json:"items"`
}

type SnowflakeWriterParameters struct {
	Database SnowflakeWriterDatabaseParameters `json:"db"`
	Tables   []SnowflakeWriterTable            `json:"tables"`
}

type SnowflakeWriterStorageTable struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Columns     []string `json:"columns"`
}

type SnowflakeWriterStorage struct {
	Input struct {
		Tables []SnowflakeWriterStorageTable `json:"tables"`
	} `json:"input"`
}

type SnowflakeWriterConfiguration struct {
	Parameters SnowflakeWriterParameters `json:"parameters"`
	Storage    SnowflakeWriterStorage    `json:"storage"`
}

type ProvisionSnowflakeResponse struct {
	Status      string `json:"status"`
	Credentials struct {
		ID          int    `json:"id"`
		HostName    string `json:"hostname"`
		Port        int    `json:"port"`
		Database    string `json:"db"`
		Schema      string `json:"schema"`
		Warehouse   string `json:"warehouse"`
		Username    string `json:"user"`
		Password    string `json:"password"`
		WorkspaceID int    `json:"workspaceId"`
	} `json:"credentials"`
}

//SnowflakeWriter is the data model for Snowflake Writers within
//the Keboola Storage API.
type SnowflakeWriter struct {
	ID            string                       `json:"id,omitempty"`
	Name          string                       `json:"name"`
	Description   string                       `json:"description"`
	Configuration SnowflakeWriterConfiguration `json:"configuration"`
}

func resourceKeboolaSnowflakeWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeWriterCreate,
		Read:   resourceKeboolaSnowflakeWriterRead,
		Update: resourceKeboolaSnowflakeWriterUpdate,
		Delete: resourceKeboolaSnowflakeWriterDelete,

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

func resourceKeboolaSnowflakeWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Snowflake Writer in Keboola.")

	createWriterForm := url.Values{}
	createWriterForm.Add("name", d.Get("name").(string))
	createWriterForm.Add("description", d.Get("description").(string))

	createWriterBuffer := bytes.NewBufferString(createWriterForm.Encode())

	client := meta.(*KbcClient)
	createResponse, err := client.PostToStorage("storage/components/keboola.wr-db-snowflake/configs", createWriterBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	var createWriterResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createWriterResult)

	if err != nil {
		return err
	}

	createAccessTokenForm := url.Values{}
	createAccessTokenForm.Add("description", fmt.Sprintf("wrdbsnowflake_%s", string(createWriterResult.ID)))
	createAccessTokenForm.Add("canManageBuckets", "1")

	createAccessTokenBuffer := bytes.NewBufferString(createAccessTokenForm.Encode())

	createAccessTokenResponse, err := client.PostToStorage("storage/tokens", createAccessTokenBuffer)

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}

	provisionSnowflakeBuffer := bytes.NewBufferString("{ \"type\": \"writer\" }")
	provisionSnowflakeResponse, err := client.PostToSyrup("provisioning/snowflake", provisionSnowflakeBuffer)

	if hasErrors(err, provisionSnowflakeResponse) {
		return extractError(err, provisionSnowflakeResponse)
	}

	var provisionedSnowflake ProvisionSnowflakeResponse

	provisionedSnowflakeDecoder := json.NewDecoder(provisionSnowflakeResponse.Body)
	err = provisionedSnowflakeDecoder.Decode(&provisionedSnowflake)

	if err != nil {
		return err
	}

	if provisionedSnowflake.Status != "ok" {
		return fmt.Errorf("Unable to provision Snowflake instance")
	}

	snowflakeCredentials := SnowflakeWriterConfiguration{}
	snowflakeCredentials.Parameters.Database.HostName = provisionedSnowflake.Credentials.HostName
	snowflakeCredentials.Parameters.Database.Port = strconv.Itoa(provisionedSnowflake.Credentials.Port)
	snowflakeCredentials.Parameters.Database.Database = provisionedSnowflake.Credentials.Database
	snowflakeCredentials.Parameters.Database.Schema = provisionedSnowflake.Credentials.Schema
	snowflakeCredentials.Parameters.Database.Username = provisionedSnowflake.Credentials.Username
	snowflakeCredentials.Parameters.Database.Password = provisionedSnowflake.Credentials.Password
	snowflakeCredentials.Parameters.Database.Driver = "snowflake"

	snowflakeCredentialsJSON, err := json.Marshal(snowflakeCredentials)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("configuration", string(snowflakeCredentialsJSON))
	updateCredentialsForm.Add("changeDescription", "Update credentials")

	updateCredentialsBuffer := bytes.NewBufferString(updateCredentialsForm.Encode())

	updateCredentialsResponse, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-snowflake/configs/%s", string(createWriterResult.ID)), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	d.SetId(string(createWriterResult.ID))

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaSnowflakeWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Snowflake Writers from Keboola.")

	client := meta.(*KbcClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResponse)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	d.Set("id", snowflakeWriter.ID)
	d.Set("name", snowflakeWriter.Name)
	d.Set("description", snowflakeWriter.Description)

	return nil
}

func resourceKeboolaSnowflakeWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Writer in Keboola.")

	//TODO: Allow updating of description and name, as both can be updated in the UI

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaSnowflakeWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Snowflake Writer in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
