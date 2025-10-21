.PHONY: tidy, proto
	
docker-down:
	docker compose down

proto:
	protoc --go_out=./proto --go-grpc_out=./proto proto/cyberday.proto

tidy:
	go mod tidy



docker-mv1:
	docker compose up --build db1 consumer-group-1 riploy


docker-mv2:
	docker compose up --build db2 consumer-group-2 falabellox


docker-mv3:
	docker compose up --build parisio db3 consumer-group-3


build-mv4:
	docker compose up --build broker 

docker-mv4:
	docker compose up broker 


reporte:
	@cat Broker/Reporte.txt