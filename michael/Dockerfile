FROM golang:1.25-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

RUN apk add --no-cache protoc
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

COPY proto/ ./proto/
RUN protoc --go_out=. --go-grpc_out=. proto/atraco.proto

COPY michael/michael.go .
RUN go build -o client michael.go

CMD ["./client"]