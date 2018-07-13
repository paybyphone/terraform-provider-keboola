package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

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
			"port":            provisionedSnowflake.Credentials.Port,
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
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := buffer.FromForm(createWriterForm)

	createResponse, err := client.PostToStorage("storage/components/keboola.wr-db-snowflake/configs", createWriterBuffer)

	if hasErrors(err, createResponse) {
		return "", extractError(err, createResponse)
	}

	var createWriterResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createWriterResult)

	if err != nil {
		return "", err
	}

	return string(createWriterResult.ID), nil
}

func createSnowflakeAccessToken(snowflakeID string, client *KBCClient) error {
	createAccessTokenForm := url.Values{}
	createAccessTokenForm.Add("description", fmt.Sprintf("wrdbsnowflake_%s", snowflakeID))
	createAccessTokenForm.Add("canManageBuckets", "1")

	createAccessTokenBuffer := buffer.FromForm(createAccessTokenForm)

	createAccessTokenResponse, err := client.PostToStorage("storage/tokens", createAccessTokenBuffer)

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}

	return nil
}

func provisionSnowflakeInstance(client *KBCClient) (provisionedSnowflakeResponse *ProvisionSnowflakeResponse, err error) {
	provisionSnowflakeBuffer := bytes.NewBufferString("{ \"type\": \"writer\" }")
	provisionSnowflakeResponse, err := client.PostToSyrup("provisioning/snowflake", provisionSnowflakeBuffer)

	if hasErrors(err, provisionSnowflakeResponse) {
		return nil, extractError(err, provisionSnowflakeResponse)
	}

	var provisionedSnowflake ProvisionSnowflakeResponse

	provisionedSnowflakeDecoder := json.NewDecoder(provisionSnowflakeResponse.Body)
	err = provisionedSnowflakeDecoder.Decode(&provisionedSnowflake)

	if err != nil {
		return nil, err
	}

	if provisionSnowflakeResponse.StatusCode < 200 || provisionSnowflakeResponse.StatusCode > 299 {
		return nil, fmt.Errorf("unable to provision Snowflake instance (status code: %v)", provisionSnowflakeResponse.StatusCode)
	}

	return &provisionedSnowflake, nil
}

func mapSnowflakeCredentialsToConfiguration(source map[string]interface{}) SnowflakeWriterDatabaseParameters {
	databaseParameters := SnowflakeWriterDatabaseParameters{}

	if val, ok := source["hostname"]; ok { databaseParameters.HostName = val.(string) }
	if val, ok := source["port"]; ok { databaseParameters.Port = val.(string) }
	if val, ok := source["database"]; ok { databaseParameters.Database = val.(string) }
	if val, ok := source["schema"]; ok { databaseParameters.Schema = val.(string) }
	if val, ok := source["warehouse"]; ok { databaseParameters.Warehouse = val.(string) }
	if val, ok := source["username"]; ok { databaseParameters.Username = val.(string) }
	if val, ok := source["hashed_password"]; ok { databaseParameters.EncryptedPassword = val.(string) }

	databaseParameters.Driver = "snowflake"

	return databaseParameters
}

func createSnowflakeCredentialsConfiguration(snowflakeCredentials map[string]interface{}, createdSnowflakeID string, client *KBCClient) error {
	snowflakeWriterConfiguration := SnowflakeWriterConfiguration{}

	snowflakeWriterConfiguration.Parameters.Database = mapSnowflakeCredentialsToConfiguration(snowflakeCredentials)

	snowflakeWriterConfigurationJSON, err := json.Marshal(snowflakeWriterConfiguration)

	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(snowflakeWriterConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", createdSnowflakeID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
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
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
