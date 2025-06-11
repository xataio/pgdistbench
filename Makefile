.PHONY: help
help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build:  k8runner benchdriver ## Build the k8runner binary

.PHONY: k8runner
k8runner: ## Build the k8runner binary
	go build ./cmd/k8runner
	
.PHONY: benchdriver
benchdriver: ## Build the benchdriver binary
	go build ./cmd/benchdriver

.PHONY: build_driver
build_driver:  ## Build the benchdriver binary
	go build ./cmd/benchdriver

.PHONY: docker_build
docker_build:  ## Build the benchdriver docker image
	docker build -t benchdriver:latest .
