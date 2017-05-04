package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccCSVImportExtractor_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckCSVImportExtractorDestroy,
			testAccCheckStorageBucketDestroy,
		),
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testCSVImportExtractorBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_csvimport_extractor.test_extractor", "name", "test_extractor"),
					resource.TestCheckResourceAttr("keboola_csvimport_extractor.test_extractor", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_csvimport_extractor.test_extractor", "destination", "out.foo.bar"),
					resource.TestCheckResourceAttr("keboola_csvimport_extractor.test_extractor", "delimiter", "%"),
					resource.TestCheckResourceAttr("keboola_csvimport_extractor.test_extractor", "enclosure", "'"),
				),
			},
		},
	})
}

func testAccCheckCSVImportExtractorDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_csvimport_extractor" {
			continue
		}

		storageTableURI := fmt.Sprintf("storage/components/keboola.csv-import/configs/%s", rs.Primary.ID)
		fmt.Println(storageTableURI)
		getResp, err := client.GetFromStorage(storageTableURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("CSV import extractor still exists")
		}
	}

	return nil
}

const testCSVImportExtractorBasic = `
	resource "keboola_csvimport_extractor" "test_extractor" {
		name = "test_extractor"
		description = "test description"
		destination = "out.foo.bar"
		delimiter = "%"
		enclosure = "'"
		primaryKey = [ "baz" ]
	}`
