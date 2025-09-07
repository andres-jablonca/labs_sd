docker-lester: 
	@sudo docker compose up --build lester 

docker-franklin:
	@sudo docker compose up --build franklin

docker-trevor:
	@sudo docker compose up --build trevor

docker-michael:
	@sudo docker compose up --build michael
	@sudo docker cp Michael:/app/reporte.txt ./reporte.txt

docker-turnoff:
	@sudo docker compose down

proto:
	@protoc --go_out=. --go-grpc_out=. ./franklin/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./trevor/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./lester/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./michael/proto/atraco.proto