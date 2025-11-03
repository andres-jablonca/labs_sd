.PHONY: tidy, proto
	
docker-down:
	docker compose down

proto:
	protoc --go_out=./proto --go-grpc_out=./proto proto/aeropuerto.proto

tidy:
	go mod tidy


docker-mv1:
	docker compose up --build broker coordinador mr1 mr2


docker-mv2:
	docker compose up --build ryw1 db1 consenso1


docker-mv3:
	docker compose up --build ryw2 db2 consenso2


docker-mv4:
	docker compose up --build ryw3 db3 consenso3


reporte:
	@cat Broker/Reporte.txt