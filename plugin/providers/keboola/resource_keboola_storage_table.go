package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"mime/multipart"
	"net/url"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

//StorageTable is the data model for Storage Tables within
//the Keboola Storage API.
type StorageTable struct {
	ID             string   `json:"id,omitempty"`
	Name           string   `json:"name"`
	Delimiter      string   `json:"delimiter"`
	Enclosure      string   `json:"enclosure,omitempty"`
	Transactional  bool     `json:"transactional,omitempty"`
	Columns        []string `json:"columns"`
	PrimaryKey     []string `json:"primaryKey"`
	IndexedColumns []string `json:"indexedColumns"`
}

//UploadFileResult contains the id of the CSV file uploaded to AWS S3.
type UploadFileResult struct {
	ID int `json:"id"`
}

//JobStatus contains the job status and results for the table load.
type JobStatus struct {
	ID      int    `json:"id"`
	URL     string `json:"url"`
	Status  string `json:"status"`
	Results struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"results"`
}

func resourceKeboolaStorageTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaStorageTableCreate,
		Read:   resourceKeboolaStorageTableRead,
		Delete: resourceKeboolaStorageTableDelete,

		Schema: map[string]*schema.Schema{
			"bucket_id": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"delimiter": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"enclosure": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"transactional": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
			},
			"primaryKey": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"columns": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"indexedColumns": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}

func resourceKeboolaStorageTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Storage Table in Keboola.")

	client := meta.(*KbcClient)
	columns := AsStringArray(d.Get("columns").([]interface{}))

	uploadFileBuffer := &bytes.Buffer{}
	writer := multipart.NewWriter(uploadFileBuffer)
	writer.SetBoundary("----terraform-provider-keboola----")
	writer.WriteField("name", "from-text-input.csv")
	writer.WriteField("data", strings.Join(columns, ","))
	writer.Close()

	uploadFileResp, err := client.PostToFileImport("upload-file", uploadFileBuffer)

	if err != nil {
		return err
	}

	var uploadFileRes UploadFileResult

	uploadFileDecoder := json.NewDecoder(uploadFileResp.Body)
	err = uploadFileDecoder.Decode(&uploadFileRes)

	if err != nil {
		return err
	}

	fileID := uploadFileRes.ID

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("primaryKey", strings.Join(AsStringArray(d.Get("primaryKey").([]interface{})), ","))
	form.Add("indexedColumns", strings.Join(AsStringArray(d.Get("indexedColumns").([]interface{})), ","))
	form.Add("dataFileId", strconv.Itoa(fileID))

	if d.Get("delimiter") != "" {
		form.Add("delimiter", d.Get("delimiter").(string))
	} else {
		form.Add("delimiter", ",")
	}

	if d.Get("enclosure") != "" {
		form.Add("enclosure", d.Get("enclosure").(string))
	} else {
		form.Add("enclosure", "\"")
	}

	formdataBuffer := bytes.NewBufferString(form.Encode())

	bucketID := d.Get("bucket_id").(string)

	loadTableResp, err := client.PostToStorage(fmt.Sprintf("storage/buckets/%s/tables-async", bucketID), formdataBuffer)

	if err != nil {
		return err
	}

	var loadTableRes UploadFileResult

	decoder := json.NewDecoder(loadTableResp.Body)
	err = decoder.Decode(&loadTableRes)

	if err != nil {
		return err
	}

	tableLoadStatus := "waiting"

	var tabeLoadJobStatusRes JobStatus

	for tableLoadStatus != "success" && tableLoadStatus != "error" {
		jobStatusResp, err := client.GetFromStorage(fmt.Sprintf("storage/jobs/%v", loadTableRes.ID))

		if err != nil {
			return err
		}

		decoder := json.NewDecoder(jobStatusResp.Body)
		err = decoder.Decode(&tabeLoadJobStatusRes)

		if err != nil {
			return err
		}

		tableLoadStatus = tabeLoadJobStatusRes.Status
	}

	d.SetId(tabeLoadJobStatusRes.Results.ID)

	return resourceKeboolaStorageTableRead(d, meta)
}

func resourceKeboolaStorageTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Storage Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	bucketID := d.Get("bucket_id").(string)

	client := meta.(*KbcClient)
	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/tables/%s.%s", bucketID, d.Get("name")))

	if err != nil {
		return err
	}

	var storageTable StorageTable

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&storageTable)

	if err != nil {
		return err
	}

	if storageTable.ID == d.Id() {
		d.Set("id", storageTable.ID)
		d.Set("name", storageTable.Name)
		d.Set("delimiter", storageTable.Delimiter)
		d.Set("enclosure", storageTable.Enclosure)
		d.Set("transactional", storageTable.Transactional)
		d.Set("primaryKey", storageTable.PrimaryKey)
		d.Set("indexedColumns", storageTable.IndexedColumns)
		d.Set("columns", storageTable.Columns)
	}

	return nil
}

func resourceKeboolaStorageTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Storage Table in Keboola: %s", d.Id())

	bucketID := d.Get("bucket_id").(string)

	client := meta.(*KbcClient)
	_, err := client.DeleteFromStorage(fmt.Sprintf("storage/buckets/%s/tables/%s", bucketID, d.Id()))

	if err != nil {
		return fmt.Errorf("Error deleting Storage Table: %s", err)
	}

	d.SetId("")

	return nil
}
