#!/bin/bash
# Build cruisereplay command-line tool for 64-bit MacOS and Linux

VERSION=$(git describe --long --dirty --tags)
GOOS=darwin GOARCH=amd64 go build -o "cruisereplay-${VERSION}-darwin-amd64" main.go || exit 1
GOOS=linux GOARCH=amd64 go build -o "cruisereplay-${VERSION}-linux-amd64" main.go || exit 1
#gzip "cruisereplay-${VERSION}-darwin-amd64"
#gzip "cruisereplay-${VERSION}-linux-amd64"
