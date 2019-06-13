package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccGoodDataWriter_v3_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckGoodDataWriterV3Destroy,
		),
		Steps: []resource.TestStep{
			{
				Config: testGoodDataWriterV3Basic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_gooddata_writer_v3.test_config", "name", "test name"),
					resource.TestCheckResourceAttr("keboola_gooddata_writer_v3.test_config", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_gooddata_writer_v3.test_config", "project_id", "123"),
					resource.TestCheckResourceAttr("keboola_gooddata_writer_v3.test_config", "login", "some-login"),
					resource.TestCheckResourceAttr("keboola_gooddata_writer_v3.test_config", "hashed_password", "pass"),
					resource.TestCheckResourceAttr("keboola_gooddata_writer_v3.test_config", "load_only", "true"),
					resource.TestCheckResourceAttr("keboola_gooddata_writer_v3.test_config", "multi_load", "false"),
				),
			},
		},
	})
}

func testAccCheckGoodDataWriterV3Destroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_gooddata_writer_v3" {
			continue
		}

		gdUserConfigURI := fmt.Sprintf("storage/components/keboola.gooddata-writer/configs/%s", rs.Primary.ID)
		getResp, err := client.GetFromStorage(gdUserConfigURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("GoodData User Management config still exists")
		}
	}

	return nil
}

const testGoodDataWriterV3Basic = `
	resource "keboola_gooddata_writer_v3" "test_config" {
		name = "test name"
		description = "test description"
		project_id = "123"
		login = "some-login"
		hashed_password = "pass"
		load_only =  true
	}`
