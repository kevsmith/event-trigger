.PHONY: all build push

IMAGE_NAME                    = kevinob/event-trigger
IMAGE_FULL_NAME              := docker.io/$(IMAGE_NAME)
IMAGE_VERSION                := $(shell git rev-parse --short HEAD)

all: build push

build:
	podman build -t $(IMAGE_FULL_NAME):$(IMAGE_VERSION) .
	podman tag $(IMAGE_FULL_NAME):$(IMAGE_VERSION) $(IMAGE_FULL_NAME):latest

push:
	podman push $(IMAGE_FULL_NAME):$(IMAGE_VERSION)
	podman push $(IMAGE_FULL_NAME):latest