# Dockerizar server
docker-lester: 
	sudo docker compose up --build lester 

# Dockerizar worker
docker-franklin:
	sudo docker compose up --build franklin

# Dockerizar client
docker-trevor:
	sudo docker compose up --build trevor

# Dockerizar client
docker-michael:
	sudo docker compose up --build michael

# Parar todo
docker-turnoff:
	@echo "🛑 Parando toda la infraestructura..."
	sudo docker compose down

proto:
	protoc --go_out=. --go-grpc_out=. ./franklin/proto/atraco.proto
	protoc --go_out=. --go-grpc_out=. ./trevor/proto/atraco.proto
	protoc --go_out=. --go-grpc_out=. ./lester/proto/atraco.proto
	protoc --go_out=. --go-grpc_out=. ./michael/proto/atraco.proto