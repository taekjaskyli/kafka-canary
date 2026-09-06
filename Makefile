include ./Makefile.os
include ./Makefile.docker
include ./Makefile.binary

PROJECT_NAME ?= kafka-canary

.PHONY: all
all: go_build docker_build

.PHONY: test
test:
	GOTOOLCHAIN=local go test ./internal/... -tags=unit_test

.PHONY: test_e2e
test_e2e:
	GOTOOLCHAIN=local go test ./test/... -tags=e2e -count=1 -timeout 12m

.PHONY: helm_lint
helm_lint:
	helm lint deploy/helm/kafka-canary

.PHONY: clean
clean: go_clean
