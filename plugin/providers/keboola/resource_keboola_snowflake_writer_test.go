package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccSnowflakeWriter_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckSnowflakeWriterDestroy,
		),
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testSnowflakeWriterBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_snowflake_writer.test_writer", "name", "test_snowflake_writer"),
					resource.TestCheckResourceAttr("keboola_snowflake_writer.test_writer", "description", "test description"),
				),
			},
		},
	})
}

func TestAccSnowflakeWriter_Update(t *testing.T) {
	// var writer SnowflakeWriter

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckSnowflakeWriterDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testSnowflakeWriterBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_snowflake_writer.test_writer", "name", "test_snowflake_writer"),
					resource.TestCheckResourceAttr("keboola_snowflake_writer.test_writer", "description", "test description"),
				),
			},
			resource.TestStep{
				Config: testSnowflakeWriterUpdate,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_snowflake_writer.test_writer", "name", "updated_test_snowflake_writer"),
					resource.TestCheckResourceAttr("keboola_snowflake_writer.test_writer", "description", "updated test description"),
				),
			},
		},
	})
}

func testAccCheckSnowflakeWriterDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_snowflake_writer" {
			continue
		}

		SnowflakeWriterURI := fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", rs.Primary.ID)
		fmt.Println(SnowflakeWriterURI)
		getResp, err := client.GetFromStorage(SnowflakeWriterURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Snowflake Writer still exists")
		}
	}

	return nil
}

const testSnowflakeWriterBasic = `
	resource "keboola_snowflake_writer" "test_writer" {
		name = "test_snowflake_writer"
		description = "test description"
	}`

const testSnowflakeWriterUpdate = `
	resource "keboola_snowflake_writer" "test_writer" {
		name = "updated_test_snowflake_writer"
		description = "updated test description"
	}`
