MOCKS_FOLDER=mocks

.PHONY: generate-mocks
generate-mocks: generator/faker.go
	@echo "Generating mocks"
	@rm -rf $(MOCKS_FOLDER)
	@for file in $^; do mockgen -source=$$file -destination=$(MOCKS_FOLDER)/$$file; done

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
