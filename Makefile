DEBUG ?= true

build:
	@go build -o dist/raft-consensus

proto_buffers:
	@rm -f raft/pb/*.pb.go
	@protoc --go-grpc_out=./raft --go_out=./raft raft/proto/*.proto

run: build
	@./dist/raft-consensus

test:
# skip modules without test files
	@go test ./... -v -debug=$(DEBUG) | grep -v 'no test files'

clean:
	@go clean -cache -modcache
