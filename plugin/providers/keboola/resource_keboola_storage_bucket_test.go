package keboola

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccStorageBucket_Basic(t *testing.T) {
	var bucket StorageBucket

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckStorageBucketDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testStorageBucketBasic,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckStorageBucketExists("keboola_storage_bucket.test_bucket", &bucket),
					resource.TestCheckResourceAttr("keboola_storage_bucket.test_bucket", "name", "test_bucket_name"),
					resource.TestCheckResourceAttr("keboola_storage_bucket.test_bucket", "description", "test description"),
				),
			},
		},
	})
}

func testAccCheckStorageBucketExists(n string, bucket *StorageBucket) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No Record ID is set")
		}

		client := testAccProvider.Meta().(*KBCClient)
		bucketURI := fmt.Sprintf("storage/buckets/%s", rs.Primary.ID)
		fmt.Printf("Checking bucket exists at: %s\n", bucketURI)
		getResp, err := client.GetFromStorage(bucketURI)

		if err != nil {
			return err
		}

		var transBucket StorageBucket

		decoder := json.NewDecoder(getResp.Body)
		err = decoder.Decode(&transBucket)

		if err != nil {
			return err
		}

		if transBucket.ID != rs.Primary.ID {
			return fmt.Errorf("Record not found")
		}

		fmt.Printf("Bucket ID: %s\n", transBucket.ID)

		*bucket = transBucket

		return nil
	}
}

func testAccCheckStorageBucketDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_storage_bucket" {
			continue
		}

		bucketURI := fmt.Sprintf("storage/buckets/%s", rs.Primary.ID)
		fmt.Printf("Checking bucket still exists after destroy at: %s\n", bucketURI)
		getResp, err := client.GetFromStorage(bucketURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Storage bucket still exists")
		}
	}

	return nil
}

const testStorageBucketBasic = `
resource "keboola_storage_bucket" "test_bucket" {
	name = "test_bucket_name"
	description = "test description"
	stage = "out"
	backend = "snowflake"
}`
