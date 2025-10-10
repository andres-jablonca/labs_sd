FROM golang:1.25-alpine

WORKDIR /app

# Copiar archivos del nodo de base de datos
COPY Productores/productor.go ./Productores/
COPY proto/ ./proto/
COPY go.mod .
COPY go.sum .

# Descargar dependencias
RUN go mod tidy

# Compilar el nodo de base de datos
RUN cd Productores && go build -o /app/productor productor.go

# Exponer puerto (se override en docker-compose)
EXPOSE 50052

# Comando por defecto (se sobreescribe en docker-compose)
CMD ["./productor", "-id=Riploy", "-port=:50052"]