#!/bin/sh

go \
	vet \
	-all \
	-race \
	./... || exec sh -c 'echo go vet failure.; exit 1'

golangci-lint \
	run
