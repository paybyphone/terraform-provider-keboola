package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func TestAccStorageTable_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckStorageTableDestroy,
			testAccCheckStorageBucketDestroy,
		),
		Steps: []resource.TestStep{
			{
				Config: testStorageTableBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_storage_table.test_table", "name", "test_table"),
				),
			},
		},
	})
}

func testAccCheckStorageTableDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_storage_table" {
			continue
		}

		storageTableURI := fmt.Sprintf("storage/tables/%s", rs.Primary.ID)
		fmt.Println(storageTableURI)
		getResp, err := client.GetFromStorage(storageTableURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Storage table still exists")
		}
	}

	return nil
}

const testStorageTableBasic = `
	resource "keboola_storage_bucket" "test_bucket" {
		name = "test_bucket_name"
		description = "test description"
		stage = "out"
		backend = "snowflake"
	}

	resource "keboola_storage_table" "test_table" {
		bucket_id = "${keboola_storage_bucket.test_bucket.id}"
  	name = "test_table"
  	columns = [ "first", "second", "third" ]
	}`
