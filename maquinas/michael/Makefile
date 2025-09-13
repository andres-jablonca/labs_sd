.PHONY: tidy proto docker-michael docker-off

docker-michael:
	@docker compose up --build michael
	@docker cp Michael:/app/Reporte.txt ./Reporte.txt 2>/dev/null && echo "Reporte final generado exitosamente"

ver-reporte:
	@cat Reporte.txt

docker-off:
	@docker compose down

proto:
	@protoc --go_out=. --go-grpc_out=. ./proto/atraco.proto

tidy:
	@go mod tidy