.PHONY: tidy proto docker-franklin docker-off

docker-franklin: 
	@docker compose up --build franklin 

docker-off:
	@docker compose down

proto:
	@protoc --go_out=. --go-grpc_out=. ./proto/atraco.proto

tidy:
	@go mod tidy