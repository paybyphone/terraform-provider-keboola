package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccOrchestration_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckOrchestrationDestroy,
		),
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testOrchestrationBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_orchestration.test_orchestration", "name", "test name"),
				),
			},
		},
	})
}

func testAccCheckOrchestrationDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KbcClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_orchestration" {
			continue
		}

		orchestrationURI := fmt.Sprintf("orchestrator/orchestrations/%s", rs.Primary.ID)
		fmt.Println(orchestrationURI)
		getResp, err := client.GetFromSyrup(orchestrationURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Orchestration still exists")
		}
	}

	return nil
}

const testOrchestrationBasic = `
resource "keboola_orchestration" "test_orchestration" {
	name = "test name"

	notification {
		email   = "hopefullydoesnot.exist@anywhere.cheese"
		channel = "error"
	}
}`
