orden de comandos:

proto:             protoc --go_out=./proto --go-grpc_out=./proto proto/cyberday.proto
Al cambiar un .go: go mod init lab2
                   go mod tidy

Broker Central:    go run Broker/main.go
DB1:               go run DB_nodo/main.go -id DB1 -port :50061
DB2:               go run DB_nodo/main.go -id DB2 -port :50062
DB3:               go run DB_nodo/main.go -id DB3 -port :50063
Productor Riploy:         go run Productor/main.go -id Riploy -port :50052
Productor Parisio:         go run Productor/main.go -id Parisio -port :50053
Productor Fallabellox:         go run Productor/main.go -id Fallabellox -port :50054