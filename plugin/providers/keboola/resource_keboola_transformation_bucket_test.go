package keboola

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccTransformationBucket_Basic(t *testing.T) {
	var bucket TransformationBucket

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckTransformationBucketDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testBucketBasic,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckTransformationBucketExists("keboola_transformation_bucket.test_bucket", &bucket),
					resource.TestCheckResourceAttr("keboola_transformation_bucket.test_bucket", "name", "test name"),
					resource.TestCheckResourceAttr("keboola_transformation_bucket.test_bucket", "description", "test description"),
				),
			},
		},
	})
}

func TestAccTransformationBucket_Update(t *testing.T) {
	var bucket TransformationBucket

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckTransformationBucketDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testBucketBasic,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckTransformationBucketExists("keboola_transformation_bucket.test_bucket", &bucket),
					resource.TestCheckResourceAttr("keboola_transformation_bucket.test_bucket", "name", "test name"),
					resource.TestCheckResourceAttr("keboola_transformation_bucket.test_bucket", "description", "test description"),
				),
			},
			resource.TestStep{
				Config: testBucketUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckTransformationBucketExists("keboola_transformation_bucket.test_bucket", &bucket),
					resource.TestCheckResourceAttr("keboola_transformation_bucket.test_bucket", "name", "new test name"),
					resource.TestCheckResourceAttr("keboola_transformation_bucket.test_bucket", "description", "new test description"),
				),
			},
		},
	})
}

func testAccCheckTransformationBucketExists(n string, bucket *TransformationBucket) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]

		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No Record ID is set")
		}

		client := testAccProvider.Meta().(*KbcClient)
		bucketURI := fmt.Sprintf("storage/components/transformation/configs/%s", rs.Primary.ID)
		fmt.Printf("Checking bucket exists at: %s\n", bucketURI)
		getResp, err := client.GetFromStorage(bucketURI)

		if err != nil {
			return err
		}

		var transBucket TransformationBucket

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

func testAccCheckTransformationBucketDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KbcClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_transformation_bucket" {
			continue
		}

		bucketURI := fmt.Sprintf("storage/components/transformation/configs/%s", rs.Primary.ID)
		fmt.Printf("Checking bucket still exists after destroy at: %s\n", bucketURI)
		getResp, err := client.GetFromStorage(bucketURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Transformation bucket still exists")
		}
	}

	return nil
}

const testBucketBasic = `
resource "keboola_transformation_bucket" "test_bucket" {
	name = "test name"
	description = "test description"
}`

const testBucketUpdate = `
resource "keboola_transformation_bucket" "test_bucket" {
	name = "new test name"
	description = "new test description"
}`
