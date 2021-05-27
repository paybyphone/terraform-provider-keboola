package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func TestAccPostgresqlWriter_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckPostgresqlWriterDestroy,
		),
		Steps: []resource.TestStep{
			{
				Config: testPostgresqlWriterBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_postgresql_writer.test_writer", "name", "test_postgresql_writer"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer.test_writer", "description", "test description"),
				),
			},
		},
	})
}

func TestAccPostgresqlWriter_Update(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlWriterDestroy,
		Steps: []resource.TestStep{
			{
				Config: testPostgresqlWriterBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_postgresql_writer.test_writer", "name", "test_postgresql_writer"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer.test_writer", "description", "test description"),
				),
			},
			{
				Config: testPostgresqlWriterUpdate,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_postgresql_writer.test_writer", "name", "updated_test_postgresql_writer"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer.test_writer", "description", "updated test description"),
				),
			},
		},
	})
}

func testAccCheckPostgresqlWriterDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_postgresql_writer" {
			continue
		}

		PostgresqlWriterURI := fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", rs.Primary.ID)
		fmt.Println(PostgresqlWriterURI)
		getResp, err := client.GetFromStorage(PostgresqlWriterURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Postgresql Writer still exists")
		}
	}

	return nil
}

const testPostgresqlWriterBasic = `
	resource "keboola_postgresql_writer" "test_writer" {
		name = "test_postgresql_writer"
		description = "test description"
	}`

const testPostgresqlWriterUpdate = `
	resource "keboola_postgresql_writer" "test_writer" {
		name = "updated_test_postgresql_writer"
		description = "updated test description"
	}`
