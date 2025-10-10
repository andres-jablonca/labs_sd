FROM golang:1.25-alpine

WORKDIR /app

# Copiar archivos del nodo de base de datos
COPY Databases/db.go ./Databases/
COPY proto/ ./proto/
COPY go.mod .
COPY go.sum .

# Descargar dependencias
RUN go mod tidy

# Compilar el nodo de base de datos
RUN cd Databases && go build -o /app/dbnodo db.go

# Exponer puerto (se override en docker-compose)
EXPOSE 50061

# Comando por defecto (se sobreescribe en docker-compose)
CMD ["./dbnodo", "-id=DB1", "-port=:50061"]