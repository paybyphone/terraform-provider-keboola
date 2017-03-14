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
	Items        []SnowflakeWriterTableItem `json:"items"`
}

type SnowflakeWriterParameters struct {
	Database SnowflakeWriterDatabaseParameters `json:"db"`
	Tables   []SnowflakeWriterTable            `json:"tables"`
}

type SnowflakeWriterConfiguration struct {
	Parameters SnowflakeWriterParameters `json:"parameters"`
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
	log.Print("[INFO] Creating Snowflake Writer in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	formdataBuffer := bytes.NewBufferString(form.Encode())

	client := meta.(*KbcClient)
	postResp, err := client.PostToStorage("storage/components/keboola.wr-db-snowflake/configs", formdataBuffer)

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

	accessTokenForm := url.Values{}
	accessTokenForm.Add("description", fmt.Sprintf("wrdbsnowflake_%s", string(createRes.ID)))
	accessTokenForm.Add("canManageBuckets", "1")

	accessTokenFormBuffer := bytes.NewBufferString(accessTokenForm.Encode())

	_, err = client.PostToStorage("storage/tokens", accessTokenFormBuffer)

	if hasErrors(err, postResp) {
		return extractError(err, postResp)
	}

	provisionBuffer := bytes.NewBufferString("{ \"type\": \"writer\" }")
	provisionResp, err := client.PostToSyrup("provisioning/snowflake", provisionBuffer)

	if hasErrors(err, postResp) {
		return extractError(err, postResp)
	}

	var provisionedSnowflake ProvisionSnowflakeResponse

	provisionDecoder := json.NewDecoder(provisionResp.Body)
	err = provisionDecoder.Decode(&provisionedSnowflake)

	if err != nil {
		return err
	}

	if provisionedSnowflake.Status != "ok" {
		return fmt.Errorf("Unable to provision Snowflake instance")
	}

	credentials := SnowflakeWriterConfiguration{}
	credentials.Parameters.Database.HostName = provisionedSnowflake.Credentials.HostName
	credentials.Parameters.Database.Port = strconv.Itoa(provisionedSnowflake.Credentials.Port)
	credentials.Parameters.Database.Database = provisionedSnowflake.Credentials.Database
	credentials.Parameters.Database.Schema = provisionedSnowflake.Credentials.Schema
	credentials.Parameters.Database.Username = provisionedSnowflake.Credentials.Username
	credentials.Parameters.Database.Password = provisionedSnowflake.Credentials.Password
	credentials.Parameters.Database.Driver = "snowflake"

	credsJSON, err := json.Marshal(credentials)

	if err != nil {
		return err
	}

	updateCredsForm := url.Values{}
	updateCredsForm.Add("configuration", string(credsJSON))
	updateCredsForm.Add("changeDescription", "Update credentials")

	updateCredsBuffer := bytes.NewBufferString(updateCredsForm.Encode())

	putResp, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-snowflake/configs/%s", string(createRes.ID)), updateCredsBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaSnowflakeWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Snowflake Writers from Keboola.")

	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	d.Set("id", snowflakeWriter.ID)
	d.Set("name", snowflakeWriter.Name)
	d.Set("description", snowflakeWriter.Description)
	//d.Set("configuration", snowflakeWriter.Configuration)

	return nil
}

func resourceKeboolaSnowflakeWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating Snowflake Writer in Keboola.")

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaSnowflakeWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Snowflake Writer in Keboola: %s", d.Id())

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
