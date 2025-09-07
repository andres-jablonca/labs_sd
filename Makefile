# Dockerizar server
docker-lester: 
	@sudo docker compose up --build lester 

# Dockerizar worker
docker-franklin:
	@sudo docker compose up --build franklin

# Dockerizar client
docker-trevor:
	@sudo docker compose up --build trevor

# Dockerizar client
docker-michael:
	@sudo docker compose up --build michael
	@sudo docker cp Michael:/app/reporte.txt ./reporte.txt

# Parar todo
docker-turnoff:
	@sudo docker compose down

proto:
	@protoc --go_out=. --go-grpc_out=. ./franklin/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./trevor/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./lester/proto/atraco.proto
	@protoc --go_out=. --go-grpc_out=. ./michael/proto/atraco.proto