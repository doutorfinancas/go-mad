.PHONY: hook-setup
hook-setup:
	pre-commit install

.PHONY: test
test:
	TEST_MODE=full go test -v ./...
	go vet -printf=false ./...

.PHONY: test-cover
test-cover:
	TEST_MODE=full go test -coverprofile=c.out -v ./...
	go tool cover -html=c.out -o coverage.html
	go vet -printf=false ./...
