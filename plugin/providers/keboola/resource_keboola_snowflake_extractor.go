package keboola

import (
    "encoding/json"
    "fmt"
    "log"
    "net/url"

    "github.com/hashicorp/terraform/helper/schema"
    "github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)


type SnowflakeExtractor struct {
    ID            string                            `json:"id,omitempty"`
    Name          string                            `json:"name"`
    Description   string                            `json:"description"`
    Configuration SnowflakeExtractorConfiguration   `json:"configuration"`
}

type SnowflakeExtractorConfiguration struct {
    Parameters SnowflakeExtractorParameters `json:"parameters"`
}

type SnowflakeExtractorParameters struct {
    Database SnowflakeDatabaseParameters    `json:"db"`
    Tables   []SnowflakeExtractorTable      `json:"tables,omitempty"`
}

type SnowflakeExtractorTable struct {
    ID              int                             `json:"id"`
    Name            string                          `json:"name"`
    Enabled         bool                            `json:"enabled,omitempty"`
    Incremental     bool                            `json:"incremental"`
    OutputTable     string                          `json:"outputTable"`
    InputTable      *SnowflakeExtractorInputTable   `json:"table,omitempty"`
    PrimaryKey      []string                        `json:"primaryKey,omitempty"`
    Query           string                          `json:"query,omitempty"`
    Columns         []string                        `json:"columns,omitempty"`
}

type SnowflakeExtractorInputTable struct {
    Schema      string          `json:"schema"`
    TableName   string          `json:"tableName"`
}


func resourceKeboolaSnowflakeExtractor() *schema.Resource {
    return &schema.Resource{
        Create: resourceKeboolaSnowflakeExtractorCreate,
        Read:   resourceKeboolaSnowflakeExtractorRead,
        Update: resourceKeboolaSnowflakeExtractorUpdate,
        Delete: resourceKeboolaSnowflakeExtractorDelete,
        Importer: &schema.ResourceImporter{
            State: schema.ImportStatePassthrough,
        },

        Schema: map[string]*schema.Schema{
            "name": {
                Type:     schema.TypeString,
                Required: true,
            },
            "description": {
                Type:     schema.TypeString,
                Optional: true,
            },
            "snowflake_db_parameters": &snowflakeDBParametersSchema,
        },
    }
}

func resourceKeboolaSnowflakeExtractorCreate(d *schema.ResourceData, meta interface{}) error {
    log.Println("[INFO] Creating Snowflake Extractor in Keboola.")

    client := meta.(*KBCClient)

    d.Partial(true)

    createExtractorForm := url.Values{}
    createExtractorForm.Add("name", d.Get("name").(string))
    createExtractorForm.Add("description", d.Get("description").(string))

    createExtractorBuffer := buffer.FromForm(createExtractorForm)

    createResponse, err := client.PostToStorage("storage/components/keboola.ex-db-snowflake/configs", createExtractorBuffer)

    if hasErrors(err, createResponse) {
        return extractError(err, createResponse)
    }

    var createResult CreateResourceResult

    decoder := json.NewDecoder(createResponse.Body)
    err = decoder.Decode(&createResult)

    if err != nil {
        return err
    }

    createdSnowflakeID := string(createResult.ID)

    d.SetPartial("name")
    d.SetPartial("description")

    snowflakeDatabaseCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})

    err = createSnowflakeExtractorCredentialsConfiguration(snowflakeDatabaseCredentials, createdSnowflakeID, client)

    if err != nil {
        return err
    }

    d.SetPartial("snowflake_db_parameters")

    d.SetId(createdSnowflakeID)

    d.Partial(false)

    return resourceKeboolaSnowflakeExtractorRead(d, meta)
}

func createSnowflakeExtractorCredentialsConfiguration(snowflakeCredentials map[string]interface{}, createdSnowflakeID string, client *KBCClient) error {
    snowflakeExtractorConfiguration := SnowflakeExtractorConfiguration{}

    snowflakeExtractorConfiguration.Parameters.Database = mapSnowflakeCredentialsToConfiguration(snowflakeCredentials)

    snowflakeWriterConfigurationJSON, err := json.Marshal(snowflakeExtractorConfiguration)

    if err != nil {
        return err
    }

    updateConfigurationRequestForm := url.Values{}
    updateConfigurationRequestForm.Add("configuration", string(snowflakeWriterConfigurationJSON))
    updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

    updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

    updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", createdSnowflakeID), updateConfigurationRequestBuffer)

    if hasErrors(err, updateConfigurationResponse) {
        return extractError(err, updateConfigurationResponse)
    }

    return nil
}

func resourceKeboolaSnowflakeExtractorRead(d *schema.ResourceData, meta interface{}) error {
    log.Println("[INFO] Reading Snowflake Extractor from Keboola.")

    if d.Id() == "" {
        return nil
    }

    client := meta.(*KBCClient)
    getSnowflakeExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))


    if hasErrors(err, getSnowflakeExtractorResponse) {
        if getSnowflakeExtractorResponse.StatusCode == 404 {
            d.SetId("")
            return nil
        }

        return extractError(err, getSnowflakeExtractorResponse)
    }

    var SnowflakeExtractor SnowflakeExtractor

    decoder := json.NewDecoder(getSnowflakeExtractorResponse.Body)
    err = decoder.Decode(&SnowflakeExtractor)

    if err != nil {
        return err
    }

    d.Set("id", SnowflakeExtractor.ID)
    d.Set("name", SnowflakeExtractor.Name)
    d.Set("description", SnowflakeExtractor.Description)

    dbParameters := make(map[string]interface{})

    databaseCredentials := SnowflakeExtractor.Configuration.Parameters.Database

    dbParameters["hostname"] = databaseCredentials.HostName
    dbParameters["port"] = databaseCredentials.Port
    dbParameters["database"] = databaseCredentials.Database
    dbParameters["schema"] = databaseCredentials.Schema
    dbParameters["warehouse"] = databaseCredentials.Warehouse
    dbParameters["username"] = databaseCredentials.Username
    dbParameters["hashed_password"] = databaseCredentials.EncryptedPassword

    d.Set("snowflake_db_parameters", dbParameters)

    return nil
}

func resourceKeboolaSnowflakeExtractorUpdate(d *schema.ResourceData, meta interface{}) error {
    log.Println("[INFO] Updating Snowflake Extractor in Keboola.")

    client := meta.(*KBCClient)

    getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

    if hasErrors(err, getExtractorResponse) {
        return extractError(err, getExtractorResponse)
    }

    var snowflakeExtractor SnowflakeExtractor

    decoder := json.NewDecoder(getExtractorResponse.Body)
    err = decoder.Decode(&snowflakeExtractor)

    if err != nil {
        return err
    }

    snowflakeCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})

    snowflakeExtractor.Configuration.Parameters.Database = mapSnowflakeCredentialsToConfiguration(snowflakeCredentials)

    snowflakeConfigJSON, err := json.Marshal(snowflakeExtractor.Configuration)

    if err != nil {
        return err
    }

    updateCredentialsForm := url.Values{}
    updateCredentialsForm.Add("name", d.Get("name").(string))
    updateCredentialsForm.Add("description", d.Get("description").(string))
    updateCredentialsForm.Add("configuration", string(snowflakeConfigJSON))
    updateCredentialsForm.Add("changeDescription", "Updated Snowflake Extractor configuration via Terraform")

    updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)

    updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()), updateCredentialsBuffer)

    if hasErrors(err, updateCredentialsResponse) {
        return extractError(err, updateCredentialsResponse)
    }

    return resourceKeboolaSnowflakeExtractorRead(d, meta)
}

func resourceKeboolaSnowflakeExtractorDelete(d *schema.ResourceData, meta interface{}) error {
    log.Printf("[INFO] Deleting Snowflake Extractor in Keboola: %s", d.Id())

    client := meta.(*KBCClient)
    destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

    if hasErrors(err, destroyResponse) {
        return extractError(err, destroyResponse)
    }

    d.SetId("")

    return nil
}
