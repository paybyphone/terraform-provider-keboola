default: build deploy

deps:
	go install github.com/hashicorp/terraform
	go install github.com/stretchr/testify

build:
ifeq ($(OS),Windows_NT)
	go build -o terraform-provider-keboola.exe .
else
	go build -o terraform-provider-keboola .
endif

test:
	go test -v ./plugin/providers/keboola/

deploy:
ifeq ($(OS),Windows_NT)
	cp terraform-provider-keboola.exe $(dir $(shell which terraform))
else
	cp terraform-provider-keboola $(dir $(shell which terraform))
endif

plan:
	@terraform plan
