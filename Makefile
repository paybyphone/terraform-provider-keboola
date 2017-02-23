default: build plan

deps:
	go install github.com/hashicorp/terraform

build:
ifeq ($(OS),Windows_NT)
	go build -o terraform-provider-keboola.exe .
else
	go build -o terraform-provider-keboola .
endif

test:
	go test -v ./plugin/providers/keboola/

plan:
	@terraform plan
