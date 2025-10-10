.PHONY: tidy proto docker-trevor docker-off

docker-trevor: 
	@docker compose up --build trevor 

docker-off:
	@docker compose down

proto:
	@protoc --go_out=. --go-grpc_out=. ./proto/atraco.proto

tidy:
	@go mod tidy