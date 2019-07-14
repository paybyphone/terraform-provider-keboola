package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccFTPExtractorFile_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckFTPExtractorFileDestroy,
		Steps: []resource.TestStep{
			{
				Config: testFTPExtractorFileBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "name", "test_extractor_file"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "description", "test file description"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "configuration", "{ \"stuff\": { } }"),
				),
			},
		},
	})
}

func TestAccFTPExtractorFile_Update(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckFTPExtractorFileDestroy,
		Steps: []resource.TestStep{
			{
				Config: testFTPExtractorFileBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "name", "test_extractor_file"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "description", "test file description"),
				),
			},
			{
				Config: testFTPExtractorFileUpdate,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "name", "test_extractor_file_updated"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "description", "test file description updated"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor_file.test_extractor_file", "configuration", "{ \"stuff\": { } }"),
				),
			},
		},
	})
}

func testAccCheckFTPExtractorFileDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_ftp_extractor_file" {
			continue
		}

		extractorID := rs.Primary.Attributes["extractor_id"]

		storageTableURI := fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s/rows/%s", extractorID, rs.Primary.ID)
		fmt.Println(storageTableURI)
		getResp, err := client.GetFromStorage(storageTableURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("FTP extractor file still exists")
		}
	}

	return nil
}

const testFTPExtractorFileBasic = `
	resource "keboola_ftp_extractor" "test_extractor" {
		name = "test_extractor"
		description = "test description"
		host = "some.ftp.site"
		port = "22"
		connection_type = "sftp"
		username = "test_username"
		hashed_password = "KBC::ProjectSecure::gibberish_goes_in_here"
	}
	
	resource "keboola_ftp_extractor_file" "test_extractor_file" {
		extractor_id = "${keboola_ftp_extractor.test_extractor.id}"
		name = "test_extractor_file"
		description = "test file description"
		configuration = "{ \"stuff\": { } }"
	}
	`

const testFTPExtractorFileUpdate = `
	resource "keboola_ftp_extractor" "test_extractor" {
		name = "test_extractor"
		description = "test description"
		host = "some.ftp.site"
		port = "22"
		connection_type = "sftp"
		username = "test_username"
		hashed_password = "KBC::ProjectSecure::gibberish_goes_in_here"
	}

	resource "keboola_ftp_extractor_file" "test_extractor_file" {
		extractor_id = "${keboola_ftp_extractor.test_extractor.id}"
		name = "test_extractor_file_updated"
		description = "test file description updated"
		configuration = "{ \"stuff\": { } }"
	}`
