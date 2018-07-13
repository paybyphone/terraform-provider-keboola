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
	"time"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

// TODO: Break this method up in to its component steps
func resourceKeboolaStorageTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Storage Table in Keboola.")

	client := meta.(*KBCClient)
	columns := AsStringArray(d.Get("columns").(*schema.Set).List())

	uploadFileBuffer := &bytes.Buffer{}
	uploadFileRequestWriter := multipart.NewWriter(uploadFileBuffer)
	uploadFileRequestWriter.SetBoundary("----terraform-provider-keboola----")
	uploadFileRequestWriter.WriteField("name", "from-text-input.csv")
	uploadFileRequestWriter.WriteField("data", strings.Join(columns, ","))
	uploadFileRequestWriter.Close()

	uploadResponse, err := client.PostToFileImport("upload-file", uploadFileBuffer)

	if hasErrors(err, uploadResponse) {
		return extractError(err, uploadResponse)
	}

	var uploadResult UploadFileResult

	uploadResponseDecoder := json.NewDecoder(uploadResponse.Body)
	err = uploadResponseDecoder.Decode(&uploadResult)

	if err != nil {
		return err
	}

	fileID := uploadResult.ID

	loadTableForm := url.Values{}
	loadTableForm.Add("name", d.Get("name").(string))
	loadTableForm.Add("primaryKey", strings.Join(AsStringArray(d.Get("primary_key").([]interface{})), ","))
	loadTableForm.Add("dataFileId", strconv.Itoa(fileID))

	if d.Get("delimiter") != "" {
		loadTableForm.Add("delimiter", d.Get("delimiter").(string))
	} else {
		loadTableForm.Add("delimiter", ",")
	}

	if d.Get("enclosure") != "" {
		loadTableForm.Add("enclosure", d.Get("enclosure").(string))
	} else {
		loadTableForm.Add("enclosure", "\"")
	}

	loadTableBuffer := buffer.FromForm(loadTableForm)

	bucketID := d.Get("bucket_id").(string)

	loadTableResponse, err := client.PostToStorage(fmt.Sprintf("storage/buckets/%s/tables-async", bucketID), loadTableBuffer)

	if hasErrors(err, loadTableResponse) {
		return extractError(err, loadTableResponse)
	}

	var loadTableResult UploadFileResult

	loadTableDecoder := json.NewDecoder(loadTableResponse.Body)
	err = loadTableDecoder.Decode(&loadTableResult)

	if err != nil {
		return err
	}

	loadStatus := "waiting"

	var table StorageJobStatus

	for loadStatus != "success" && loadStatus != "error" {
		loadStatusResp, err := client.GetFromStorage(fmt.Sprintf("storage/jobs/%v", loadTableResult.ID))

		if hasErrors(err, loadStatusResp) {
			return extractError(err, loadStatusResp)
		}

		jobStatusDecoder := json.NewDecoder(loadStatusResp.Body)
		err = jobStatusDecoder.Decode(&table)

		if err != nil {
			return err
		}

		time.Sleep(250 * time.Millisecond)
		loadStatus = table.Status
	}

	indexedOnlyColumns := except(AsStringArray(d.Get("indexed_columns").([]interface{})), AsStringArray(d.Get("primary_key").([]interface{})))

	for _, indexedColumn := range indexedOnlyColumns {
		indexColResp, err := client.PostToStorage(fmt.Sprintf("storage/tables/%s/indexed-columns?name=%s", table.Results.ID, indexedColumn), buffer.Empty())

		if hasErrors(err, indexColResp) {
			return extractError(err, indexColResp)
		}
	}

	d.SetId(table.Results.ID)

	return resourceKeboolaStorageTableRead(d, meta)
}

func except(first []string, second []string) []string {
	var res []string

	for _, firstElem := range first {
		found := false
		for _, secondElem := range second {
			if firstElem == secondElem {
				found = true
				break
			}
		}

		if !found {
			res = append(res, firstElem)
		}
	}

	return res
}

func resourceKeboolaStorageTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Storage Table from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/tables/%s", d.Id()))

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

	var storageTable StorageTable

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&storageTable)

	if err != nil {
		return err
	}

	d.Set("name", storageTable.Name)
	d.Set("delimiter", storageTable.Delimiter)
	d.Set("enclosure", storageTable.Enclosure)
	d.Set("transactional", storageTable.Transactional)
	d.Set("primary_key", storageTable.PrimaryKey)
	d.Set("indexed_columns", storageTable.IndexedColumns)
	d.Set("columns", storageTable.Columns)

	return nil
}

func resourceKeboolaStorageTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Storage Table in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/tables/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
