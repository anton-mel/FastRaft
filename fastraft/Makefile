DEBUG ?= true

NODE_DRIVER_BINARY := dist/node-driver
LOAD_DRIVER_BINARY := dist/load-driver


build-node-driver:
	@echo "Building node driver binary..."
	@go build -o $(NODE_DRIVER_BINARY) ./node_driver
	@chmod +x $(NODE_DRIVER_BINARY)

build-load-driver:
	@echo "Building load driver binary..."
	@go build -o $(LOAD_DRIVER_BINARY) ./load_driver
	@chmod +x $(LOAD_DRIVER_BINARY)

build: build-node-driver build-load-driver


run-node-driver:
	@echo "Running node driver..."
	@./$(NODE_DRIVER_BINARY) $(ME) $(PEERS)

run-load-driver:
	@echo "Running load driver..."
	@./$(LOAD_DRIVER_BINARY) $(ME) $(PEERS)

proto_buffers:
	@echo "Generating protobuf files..."
	@rm -f raft/pb/*.pb.go
	@protoc --go-grpc_out=./raft --go_out=./raft raft/proto/*.proto


test:
# skip modules without test files
	@echo "Running tests..."
	@go test ./... -v -debug=$(DEBUG) | grep -v 'no test files'

clean:
	@echo "Cleaning up..."
	@go clean -cache -modcache
	@rm -f $(NODE_DRIVER_BINARY) $(LOAD_DRIVER_BINARY)
