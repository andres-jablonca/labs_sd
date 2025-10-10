FROM golang:1.25-alpine

WORKDIR /app

# Copiar archivos del broker
COPY Broker/broker.go ./Broker/
COPY proto/ ./proto/
COPY go.mod .
COPY go.sum .

# Descargar dependencias
RUN go mod tidy

# Compilar el broker
RUN cd Broker && go build -o /app/broker broker.go

# Exponer puerto
EXPOSE 50051

CMD ["./broker"]