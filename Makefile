lint:
	gofmt -s -w .
	golangci-lint run --no-config --fix --disable-all -E tagalign --timeout 10m
	golangci-lint run --fix --timeout 10m
	staticcheck ./...
