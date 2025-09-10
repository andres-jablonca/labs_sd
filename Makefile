.PHONY: tidy, proto

docker-lester: 
	@docker compose up --build lester 

docker-franklin:
	@docker compose up --build franklin

docker-trevor:
	@docker compose up --build trevor

docker-michael:
	@docker compose up --build michael
	@docker cp Michael:/app/Reporte.txt ./Reporte.txt 2>/dev/null && echo "Reporte final generado exitosamente"

ver-reporte:
	@cat Reporte.txt

docker-off:
	docker compose down

proto:
	@protoc --go_out=. --go-grpc_out=. ./franklin/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./trevor/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./lester/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./michael/proto/atraco.proto

tidy:
	@cd lester && go mod tidy
	@cd michael && go mod tidy
	@cd franklin && go mod tidy
	@cd trevor && go mod tidy