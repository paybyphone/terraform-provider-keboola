package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func TestAccFTPExtractor_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckFTPExtractorDestroy,
		Steps: []resource.TestStep{
			{
				Config: testFTPExtractorBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "name", "test_extractor"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "host", "some.ftp.site"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "port", "22"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "connection_type", "sftp"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "username", "test_username"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "hashed_password", "KBC::ProjectSecure::gibberish_goes_in_here"),
				),
			},
		},
	})
}

func TestAccFTPExtractor_Update(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckFTPExtractorDestroy,
		Steps: []resource.TestStep{
			{
				Config: testFTPExtractorBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "name", "test_extractor"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "description", "test description"),
				),
			},
			{
				Config: testFTPExtractorUpdate,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "name", "test_extractor updated"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "description", "test description updated"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "host", "some.other.ftp.site"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "port", "23"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "connection_type", "sftp"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "username", "test_username_updated"),
					resource.TestCheckResourceAttr("keboola_ftp_extractor.test_extractor", "hashed_password", "KBC::ProjectSecure::gibberish_goes_in_here_updated"),
				),
			},
		},
	})
}

func testAccCheckFTPExtractorDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_ftp_extractor" {
			continue
		}

		storageTableURI := fmt.Sprintf("storage/components/keboola.ex-ftp/configs/%s", rs.Primary.ID)
		fmt.Println(storageTableURI)
		getResp, err := client.GetFromStorage(storageTableURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("FTP extractor still exists")
		}
	}

	return nil
}

const testFTPExtractorBasic = `
	resource "keboola_ftp_extractor" "test_extractor" {
		name = "test_extractor"
		description = "test description"
		host = "some.ftp.site"
		port = "22"
		connection_type = "sftp"
		username = "test_username"
		hashed_password = "KBC::ProjectSecure::gibberish_goes_in_here"
	}`

const testFTPExtractorUpdate = `
	resource "keboola_ftp_extractor" "test_extractor" {
		name = "test_extractor updated"
		description = "test description updated"
		host = "some.other.ftp.site"
		port = "23"
		connection_type = "sftp"
		username = "test_username_updated"
		hashed_password = "KBC::ProjectSecure::gibberish_goes_in_here_updated"
	}`
