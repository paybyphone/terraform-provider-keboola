package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccSnowflakeExtractor_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckSnowflakeExtractorDestroy,
		),
		Steps: []resource.TestStep{
			{
				Config: testSnowflakeExtractorBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_snowflake_extractor.test_extractor", "name", "test_snowflake_extractor"),
					resource.TestCheckResourceAttr("keboola_snowflake_extractor.test_extractor", "description", "test description"),
				),
			},
		},
	})
}

func TestAccSnowflakeExtractor_Update(t *testing.T) {
	// var extractor SnowflakeExtractor

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckSnowflakeExtractorDestroy,
		Steps: []resource.TestStep{
			{
				Config: testSnowflakeExtractorBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_snowflake_extractor.test_extractor", "name", "test_snowflake_extractor"),
					resource.TestCheckResourceAttr("keboola_snowflake_extractor.test_extractor", "description", "test description"),
				),
			},
			{
				Config: testSnowflakeExtractorUpdate,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_snowflake_extractor.test_extractor", "name", "updated_test_snowflake_extractor"),
					resource.TestCheckResourceAttr("keboola_snowflake_extractor.test_extractor", "description", "updated test description"),
				),
			},
		},
	})
}

func testAccCheckSnowflakeExtractorDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_snowflake_extractor" {
			continue
		}

		SnowflakeExtractorURI := fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", rs.Primary.ID)
		fmt.Println(SnowflakeExtractorURI)
		getResp, err := client.GetFromStorage(SnowflakeExtractorURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("snowflake extractor still exists")
		}
	}

	return nil
}

const testSnowflakeExtractorBasic = `
    resource "keboola_snowflake_extractor" "test_extractor" {
        name = "test_snowflake_extractor"
        description = "test description"
    }`

const testSnowflakeExtractorUpdate = `
    resource "keboola_snowflake_extractor" "test_extractor" {
        name = "updated_test_snowflake_extractor"
        description = "updated test description"
    }`
