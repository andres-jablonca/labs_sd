.PHONY: tidy proto docker-lester docker-off

docker-lester: 
	@docker compose up --build lester 

docker-off:
	@docker compose down

proto:
	@protoc --go_out=. --go-grpc_out=. ./proto/atraco.proto

tidy:
	@go mod tidy