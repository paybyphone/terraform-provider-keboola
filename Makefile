version=0.3.2

default: build deploy

gets:
	go get github.com/hashicorp/terraform
	go get github.com/stretchr/testify
	go get gopkg.in/alecthomas/gometalinter.v2

deps:
	go install github.com/hashicorp/terraform
	go install github.com/stretchr/testify

build:
	GOARCH=amd64 GOOS=windows go build -o terraform-provider-keboola_windows_amd64.exe
	GOARCH=amd64 GOOS=linux go build -o terraform-provider-keboola_linux_amd64
	GOARCH=amd64 GOOS=darwin go build -o terraform-provider-keboola_darwin_amd64

test:
	go test -v ./plugin/providers/keboola/

deploy: release
ifeq ($(OS),Windows_NT)
	cp bin/terraform-provider-keboola_windows_amd64.exe $(dir $(shell which terraform))
else
	cp bin/terraform-provider-keboola_darwin_amd64 $(dir $(shell which terraform))
endif

plan:
	@terraform plan

release: test
	rm -fr bin
	mkdir -p bin/windows_amd64
	mkdir -p bin/linux_amd64
	mkdir -p bin/darwin_amd64

	GOARCH=amd64 GOOS=windows go build -o bin/windows_amd64/terraform-provider-keboola_v${version}.exe
	GOARCH=amd64 GOOS=linux go build -o bin/linux_amd64/terraform-provider-keboola_v${version}
	GOARCH=amd64 GOOS=darwin go build -o bin/darwin_amd64/terraform-provider-keboola_v${version}

	mkdir -p releases/
	zip releases/terraform-provider-keboola_windows_amd64_v${version}.zip bin/windows_amd64/terraform-provider-keboola_v${version}.exe
	zip releases/terraform-provider-keboola_linux_amd64_v${version}.zip bin/linux_amd64/terraform-provider-keboola_v${version}
	zip releases/terraform-provider-keboola_darwin_amd64_v${version}.zip bin/darwin_amd64/terraform-provider-keboola_v${version}