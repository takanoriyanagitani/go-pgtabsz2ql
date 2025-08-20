#!/bin/sh

find \
	. \
	-type f \
	-name '*.go' |
	xargs \
		gofmt \
		-s \
		-w

find \
	. \
	-type f \
	-name '*.go' |
	xargs \
        gci \
        write \
        --skip-generated \
        --section standard \
        --section default
