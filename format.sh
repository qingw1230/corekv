#!/bin/bash

go mod tidy
goimports -w .
gofmt -w .
