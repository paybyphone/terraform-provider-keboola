package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccPostgresqlWriterTables_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckPostgresqlWriterTablesDestroy,
		),
		Steps: []resource.TestStep{
			{
				Config: testPostgresqlWriterTablesBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.db_name", "test_database"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.export", "true"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.incremental", "true"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.table_id", "out.c-test_bucket_name.test_table"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.primary_key.#", "1"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.primary_key.0", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.#", "3"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.name", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.db_name", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.type", "integer"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.name", "second"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.db_name", "second"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.type", "varchar"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.size", "255"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.nullable", "false"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.name", "third"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.db_name", "db_third"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.type", "string"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.size", "100"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.nullable", "true"),
				),
			},
		},
	})
}

func TestAccPostgresqlWriterTables_Update(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlWriterTablesDestroy,
		Steps: []resource.TestStep{
			{
				Config: testPostgresqlWriterTablesBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.db_name", "test_database"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.export", "true"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.incremental", "true"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.table_id", "out.c-test_bucket_name.test_table"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.primary_key.#", "1"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.primary_key.0", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.#", "3"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.name", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.db_name", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.type", "integer"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.name", "second"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.db_name", "second"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.type", "varchar"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.size", "255"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.nullable", "false"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.name", "third"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.db_name", "db_third"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.type", "string"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.size", "100"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.2.nullable", "true"),
				),
			},
			{
				Config: testPostgresqlWriterTablesUpdate,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.db_name", "updated_test_database"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.export", "true"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.incremental", "false"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.table_id", "out.c-test_bucket_name.test_table"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.primary_key.#", "0"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.#", "2"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.name", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.db_name", "first"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.type", "integer"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.0.nullable", "true"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.name", "second"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.db_name", "second"),
					resource.TestCheckResourceAttr("keboola_postgresql_writer_tables.test_writer_tables", "table.0.column.1.type", "integer"),
				),
			},
		},
	})
}

func testAccCheckPostgresqlWriterTablesDestroy(s *terraform.State) error {
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

const testDependencies = `
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
	}

	resource "keboola_postgresql_writer" "test_writer" {
		name = "test_postgresql_writer"
		description = "test description"
	}
`

const testPostgresqlWriterTablesBasic = testDependencies + `
	resource "keboola_postgresql_writer_tables" "test_writer_tables" {
		writer_id = "${keboola_postgresql_writer.test_writer.id}"

		table {
			db_name     = "test_database"
			export      = true
			incremental = true
			table_id    = "out.c-test_bucket_name.test_table"
			primary_key = ["first"]
		
			column {
			  	name    = "first"
			  	db_name = "first"
			  	type    = "integer"
			}
			
			column {
				name     = "second"
				db_name  = "second"
				type     = "varchar"
				size     = "255"
				nullable = "false"
			}
			  
			column {
				name     = "third"
				db_name  = "db_third"
				type     = "string"
				size     = "100"
				nullable = "true"
			}
		}
	}`

const testPostgresqlWriterTablesUpdate = testDependencies + `
	resource "keboola_postgresql_writer_tables" "test_writer_tables" {
		writer_id = "${keboola_postgresql_writer.test_writer.id}"

		table {
			db_name     = "updated_test_database"
			export      = true
			incremental = false
			table_id    = "out.c-test_bucket_name.test_table"
		
			column {
				name     = "first"
				db_name  = "first"
				type     = "integer"
				nullable = "true"
			}
			
			column {
				name     = "second"
				db_name  = "second"
				type     = "integer"
			}
		}
	}`
