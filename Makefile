SHELL = /bin/bash

# Just to be sure, add the path of the binary-based go installation.
PATH := /usr/local/go/bin:$(PATH)

# Using the (above extended) path, query the GOPATH (i.e. the user's go path).
GOPATH := $(shell env PATH=$(PATH) go env GOPATH)

# Add $GOPATH/bin to path
PATH := $(GOPATH)/bin:$(PATH)

start_db: stop_db
	docker run -e ARANGO_NO_AUTH=1 -p 8529:8529 --name arangodb-instance arangodb

stop_db:
	docker stop arangodb-instance 2>/dev/null || true
	docker rm arangodb-instance 2>/dev/null || true

fmt:
	gofmt -s -w .

test:
	go test -p 4 -v ./...

lint:
	golangci-lint run

coverage:
	go test -coverprofile=c.out && go tool cover -html=c.out

start_doc:
	godoc -http=:6060

open_doc:
	gio open http://localhost:6060/pkg/github.com/heimdalr/arangodag/

clean:
	go clean -testcache
	rm -f c.out

.PHONY: start_db stop_db fmt test lint coverage start_doc open_doc clean