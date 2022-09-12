.PHONY: all build push

IMAGE_NAME                    = kevinob/event-trigger
IMAGE_FULL_NAME              := docker.io/$(IMAGE_NAME)
IMAGE_VERSION                := $(shell git rev-parse --short HEAD)

all: build push

build:
	podman build -t $(IMAGE_FULL_NAME):latest -t $(IMAGE_FULL_NAME):$(IMAGE_VERSION) .

push:
	podman push $(IMAGE_FULL_NAME):latest $(IMAGE_FULL_NAME):$(IMAGE_VERSION)