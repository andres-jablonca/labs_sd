.PHONY: tidy, proto
	
docker-down:
	docker compose down

proto:
	protoc --go_out=./proto --go-grpc_out=./proto proto/cyberday.proto

tidy:
	go mod tidy


# Solo construir 

build-mv1:
	docker compose build broker 

build-mv2:
	docker compose build db1 consumer-group-1 riploy

build-mv3:
	docker compose build db2 consumer-group-2 falabellox

build-mv4:
	docker compose build parisio db3 consumer-group-3


# Levantar contenedores ya construidos

docker-mv1:
	docker compose up broker 

docker-mv2:
	docker compose up db1 consumer-group-1 riploy


docker-mv3:
	docker compose up db2 consumer-group-2 falabellox


docker-mv4:
	docker compose up parisio db3 consumer-group-3


reporte:
	@cat Broker/Reporte.txt