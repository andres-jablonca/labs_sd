# Actualizar el módulo
go mod tidy

# 1. Inicia el Datanode
go run ./datanode/main.go 

# 2. Inicia el Broker
go run ./broker/main.go 

# 3. Inicia el Coordinador
go run ./coordinator/main.go 

# 4. Inicia el Cliente RYW
go run ./client_ryw/main.go

# 2. Recompila con el comando correcto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/aerodist.proto