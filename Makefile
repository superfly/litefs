.PHONY: default
default:

.PHONY: check
check:
	errcheck ./...
	staticcheck ./...
