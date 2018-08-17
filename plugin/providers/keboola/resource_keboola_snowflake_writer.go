package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
    "strconv"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaSnowflakeWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Snowflake Writer in Keboola.")

	client := meta.(*KBCClient)

	d.Partial(true)

	createdSnowflakeID, err := createSnowflakeWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetPartial("name")
	d.SetPartial("description")

	err = createSnowflakeAccessToken(createdSnowflakeID, client)

	if err != nil {
		return err
	}

	snowflakeDatabaseCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})

	if d.Get("provision_new_instance").(bool) == true {
		provisionedSnowflake, err := provisionSnowflakeInstance(client)

		if err != nil {
			return err
		}

		snowflakeDatabaseCredentials = map[string]interface{}{
			"hostname":        provisionedSnowflake.Credentials.HostName,
			"port":            strconv.Itoa(provisionedSnowflake.Credentials.Port),
			"database":        provisionedSnowflake.Credentials.Database,
			"schema":          provisionedSnowflake.Credentials.Schema,
			"warehouse":       provisionedSnowflake.Credentials.Warehouse,
			"username":        provisionedSnowflake.Credentials.Username,
			"hashed_password": provisionedSnowflake.Credentials.Password,
		}
	}

	err = createSnowflakeCredentialsConfiguration(snowflakeDatabaseCredentials, createdSnowflakeID, client)

	if err != nil {
		return err
	}

	d.SetPartial("snowflake_db_parameters")

	d.SetId(createdSnowflakeID)

	d.Partial(false)

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func createSnowflakeWriterConfiguration(name string, description string, client *KBCClient) (createdSnowflakeID string, err error) {
	form := url.Values{}
	form.Add("name", name)
	form.Add("description", description)

	formData := buffer.FromForm(form)

	resp, err := client.PostToStorage("storage/components/keboola.wr-db-snowflake/configs", formData)

	if hasErrors(err, resp) {
		return "", extractError(err, resp)
	}

	var writer CreateResourceResult

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return "", err
	}

	return string(writer.ID), nil
}

func createSnowflakeAccessToken(snowflakeID string, client *KBCClient) error {
	form := url.Values{}
	form.Add("description", fmt.Sprintf("wrdbsnowflake_%s", snowflakeID))
	form.Add("canManageBuckets", "1")

	formData := buffer.FromForm(form)

	resp, err := client.PostToStorage("storage/tokens", formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return nil
}

func provisionSnowflakeInstance(client *KBCClient) (provisionedSnowflakeResponse *ProvisionSnowflakeResponse, err error) {
	jsonData := bytes.NewBufferString("{ \"type\": \"writer\" }")
	resp, err := client.PostToSyrup("provisioning/snowflake", jsonData)

	if hasErrors(err, resp) {
		return nil, extractError(err, resp)
	}

	var snowflake ProvisionSnowflakeResponse

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&snowflake)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("unable to provision Snowflake instance (status code: %v)", resp.StatusCode)
	}

	return &snowflake, nil
}

func mapSnowflakeCredentialsToConfiguration(source map[string]interface{}) SnowflakeWriterDatabaseParameters {
	dbParams := SnowflakeWriterDatabaseParameters{}

	if val, ok := source["hostname"]; ok { dbParams.HostName = val.(string) }
	if val, ok := source["port"]; ok { dbParams.Port = val.(string) }
	if val, ok := source["database"]; ok { dbParams.Database = val.(string) }
	if val, ok := source["schema"]; ok { dbParams.Schema = val.(string) }
	if val, ok := source["warehouse"]; ok { dbParams.Warehouse = val.(string) }
	if val, ok := source["username"]; ok { dbParams.Username = val.(string) }
	if val, ok := source["hashed_password"]; ok { dbParams.EncryptedPassword = val.(string) }

	dbParams.Driver = "snowflake"

	return dbParams
}

func createSnowflakeCredentialsConfiguration(snowflakeCredentials map[string]interface{}, createdSnowflakeID string, client *KBCClient) error {
	snowflakeConfig := SnowflakeWriterConfiguration{}

	snowflakeConfig.Parameters.Database = mapSnowflakeCredentialsToConfiguration(snowflakeCredentials)

	snowflakeJSON, err := json.Marshal(snowflakeConfig)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(snowflakeJSON))
	form.Add("changeDescription", "Created database credentials")

	formData := buffer.FromForm(form)

	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", createdSnowflakeID), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return nil
}

func resourceKeboolaSnowflakeWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Snowflake Writers from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

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

	var writer SnowflakeWriter
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	d.Set("id", writer.ID)
	d.Set("name", writer.Name)
	d.Set("description", writer.Description)

	if d.Get("provision_new_database") == false {
		dbParams := make(map[string]interface{})

		dbCreds := writer.Configuration.Parameters.Database

		dbParams["hostname"] = dbCreds.HostName
		dbParams["port"] = dbCreds.Port
		dbParams["database"] = dbCreds.Database
		dbParams["schema"] = dbCreds.Schema
		dbParams["warehouse"] = dbCreds.Warehouse
		dbParams["username"] = dbCreds.Username
		dbParams["hashed_password"] = dbCreds.EncryptedPassword

		d.Set("snowflake_db_parameters", dbParams)
	}

	return nil
}

// TODO: Split up method in to the two API calls
func resourceKeboolaSnowflakeWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Writer in Keboola.")

	client := meta.(*KBCClient)

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer SnowflakeWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	dbParams := d.Get("snowflake_db_parameters").(map[string]interface{})

	if d.Get("provision_new_instance").(bool) == false {
		writer.Configuration.Parameters.Database = mapSnowflakeCredentialsToConfiguration(dbParams)
	}

	snowflakeConfigJSON, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("configuration", string(snowflakeConfigJSON))
	form.Add("changeDescription", "Updated Snowflake Writer configuration via Terraform")

	formData := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaSnowflakeWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Snowflake Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
