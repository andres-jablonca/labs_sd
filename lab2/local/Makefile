.PHONY: tidy, proto

docker-broker:
	docker compose up --build broker

docker-db1:
	docker compose up --build db1

docker-db2:
	docker compose up --build db2

docker-db3:
	docker compose up --build db3

docker-riploy:
	docker compose up --build riploy

docker-falabellox:
	docker compose up --build falabellox

docker-parisio:
	docker compose up --build parisio

docker-c1:
	docker compose up --build consumer-group-1

docker-c2:
	docker compose up --build consumer-group-2
	
docker-c3:
	docker compose up --build consumer-group-3
	
docker-down:
	docker compose down

docker-michael:
	@docker compose up --build michael
	@docker cp Michael:/app/Reporte.txt ./Reporte.txt 2>/dev/null && echo "Reporte final generado exitosamente"

ver-reporte:
	@cat Reporte.txt

proto:
	protoc --go_out=./proto --go-grpc_out=./proto proto/cyberday.proto

tidy:
	go mod tidy

docker-mv1:
	docker compose up --build db1 db2 db3 

docker-mv2:
	docker compose up --build consumer-group-3 consumer-group-2 consumer-group-1

docker-mv3:
	docker compose up --build parisio riploy falabellox

docker-mv4:
	docker compose up --build broker 