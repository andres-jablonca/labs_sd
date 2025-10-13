FROM golang:1.25-alpine

WORKDIR /app

# Copiar archivos del broker
COPY Broker/ ./Broker/
COPY proto/ ./proto/
COPY go.mod .
COPY go.sum .

# 💡 CORRECCIÓN: Copiar el archivo de preferencias de consumidores
# Asumimos que consumidores.csv está en el directorio raíz de la construcción de Docker.


# Descargar dependencias
RUN go mod tidy

# Compilar el broker
RUN cd Broker && go build -o /app/broker broker.go

# Exponer puerto
EXPOSE 50095

CMD ["./broker"]
