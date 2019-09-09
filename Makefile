build:
	GO111MODULE=on go build -ldflags "-w -s" ./cmd/kaf
install:
	go install -mod vendor
release:
	rm -rf dist/ && goreleaser
