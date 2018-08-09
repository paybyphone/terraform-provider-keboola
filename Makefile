version=0.1.1

default: build deploy

gets:
	go get github.com/hashicorp/terraform
	go get github.com/stretchr/testify

deps:
	go install github.com/hashicorp/terraform
	go install github.com/stretchr/testify

build:
ifeq ($(OS),Windows_NT)
	go build -o terraform-provider-keboola_v$(version).exe .
else
	go build -o terraform-provider-keboola_v$(version) .
endif

test:
	go test -v ./plugin/providers/keboola/

deploy:
ifeq ($(OS),Windows_NT)
	cp terraform-provider-keboola_v$(version).exe $(dir $(shell which terraform))
else
	cp terraform-provider-keboola_v$(version) $(dir $(shell which terraform))
endif

plan:
	@terraform plan
