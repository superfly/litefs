.PHONY: default
default:

.PHONY: docker
docker:
ifndef GITHUB_TOKEN
	$(error GITHUB_TOKEN is undefined)
endif
	docker build --build-arg GITHUB_TOKEN=${GITHUB_TOKEN} -t litefs .