build:
	@go build -o dist/raft-consensus

proto_buffers:
	@rm -f pb/*.pb.go
	@protoc --go-grpc_out=. --go_out=. proto/*.proto

run: build
	@./dist/raft-consensus
