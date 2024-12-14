FROM golang:1.23.2-alpine AS builder

RUN apk add --no-cache build-base curl protobuf

RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1 \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

# RUN make proto_buffers && make build

RUN go build -o /go/bin/app node_driver/main.go

# RUN SELF_IP=$(ifconfig | grep 'inet ' | awk '{print $2}' | grep -v '127.0.0.1' | head -n 1)
# needs to be on k8s variables

## Deploy
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /go/bin/app .
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

EXPOSE 5000

ENTRYPOINT [ "./entrypoint.sh" ]

# ENTRYPOINT [ "./app", "ME=${SELF_IP}:5000", "PEERS_IP=${PEERS_IP}"] 

# CMD ["make", "run-node-driver", "ME=${SELF_IP}:5000", "PEERS=${PEERS_IP}"]
