install-deps:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

install-plugin:
	go install ./cmd/protoc-gen-go-amqp-rpc

gen-proto:
	protoc --go_out=. --go_opt=paths=source_relative \
            rpc/rpc.proto

gen-proto-health:
	protoc --go_out=. --go_opt=paths=source_relative \
		  	--go-amqp-rpc_out=. --go-amqp-rpc_opt=paths=source_relative \
          	health/healthpb/*.proto

tidy:
	go mod tidy
	go mod vendor

gen-example:
	protoc --go_out=. --go_opt=paths=source_relative \
        --go-amqp-rpc_out=. --go-amqp-rpc_opt=paths=source_relative \
        examples/helloworld/proto/helloworld.proto


.phony: install-deps install-plugin gen-example gen-proto tidy
