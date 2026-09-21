run-example:
	@echo "Run Example..."
	go test -v -run TestExample

# Generated with protoc 35.1 and protoc-gen-go v1.36.11; other versions churn the stamps.
generate:
	@echo "Generate protobuf..."
	protoc --go_out=. --go_opt=paths=source_relative internal/kpl/messages.proto
