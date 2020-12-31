SERVICE_NAME = $(notdir $(CURDIR))
LATEST_TAG = vnext
VERSION_TAG = vnext-$(shell git rev-parse --short=7 --verify HEAD)

default: build

define build-docker-image
	docker build \
		--network=host \
		--tag plgd/$(SERVICE_NAME):$(VERSION_TAG) \
		--tag plgd/$(SERVICE_NAME):$(LATEST_TAG) \
		--target $(1) \
		.
endef

build-testcontainer:
	$(call build-docker-image,build)

build: build-testcontainer

nats:
	docker run \
	    -d \
		--network=host \
		--name=nats \
		nats

mongo:
	mkdir -p $(shell pwd)/.tmp/mongo
	docker run \
	    -d \
		--network=host \
		--name=mongo \
		mongo

env: clean nats mongo

test: clean env build
	docker run \
		--network=host \
		--mount type=bind,source="$(shell pwd)",target=/shared \
		plgd/$(SERVICE_NAME):$(VERSION_TAG) \
		go test -v ./... -covermode=atomic -coverprofile=/shared/coverage.txt

clean:
	docker rm -f mongo || true
	docker rm -f nats || true

.PHONY: build-testcontainer build-servicecontainer build test clean



