.PHONY: tidy, proto
	
docker-down:
	docker compose down

proto:
	protoc --go_out=. --go-grpc_out=. proto/aerodist.proto

tidy:
	go mod tidy


docker-mv1:
	docker compose up --build broker coordinator client_mr client_mr2


docker-mv2:
	docker compose up --build client_ryw datanode1 


docker-mv3:
	docker compose up --build client_ryw2 datanode2 


docker-mv4:
	docker compose up --build client_ryw3 datanode3 


reporte:
	@cat Broker/Reporte.txt