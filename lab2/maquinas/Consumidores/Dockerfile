FROM golang:1.25-alpine

WORKDIR /app

# Copiar archivos
COPY Consumidores/ ./Consumidores/
COPY proto/ ./proto/
COPY go.mod .
COPY go.sum .

# Crear script de inicio
RUN echo '#!/bin/sh' > /app/start-group1.sh && \
    echo './consumidor -id=C1-1 -port=:50071 &' >> /app/start-group1.sh && \
    echo './consumidor -id=C1-2 -port=:50072 &' >> /app/start-group1.sh && \
    echo './consumidor -id=C1-3 -port=:50073 &' >> /app/start-group1.sh && \
    echo './consumidor -id=C1-4 -port=:50074 &' >> /app/start-group1.sh && \
    echo 'wait' >> /app/start-group1.sh && \
    chmod +x /app/start-group1.sh

RUN echo '#!/bin/sh' > /app/start-group2.sh && \
    echo './consumidor -id=C2-1 -port=:50075 &' >> /app/start-group2.sh && \
    echo './consumidor -id=C2-2 -port=:50076 &' >> /app/start-group2.sh && \
    echo './consumidor -id=C2-3 -port=:50077 &' >> /app/start-group2.sh && \
    echo './consumidor -id=C2-4 -port=:50078 &' >> /app/start-group2.sh && \
    echo 'wait' >> /app/start-group2.sh && \
    chmod +x /app/start-group2.sh

RUN echo '#!/bin/sh' > /app/start-group3.sh && \
    echo './consumidor -id=C3-1 -port=:50079 &' >> /app/start-group3.sh && \
    echo './consumidor -id=C3-2 -port=:50080 &' >> /app/start-group3.sh && \
    echo './consumidor -id=C3-3 -port=:50081 &' >> /app/start-group3.sh && \
    echo './consumidor -id=C3-4 -port=:50082 &' >> /app/start-group3.sh && \
    echo 'wait' >> /app/start-group3.sh && \
    chmod +x /app/start-group3.sh

# Descargar dependencias y compilar
RUN go mod tidy
RUN cd Consumidores && go build -o /app/consumidor consumidor.go

# Exponer puertos
EXPOSE 50071-50082

# Usar el script CORREGIDO - cambia a start-group1.sh
CMD ["/app/start-group1.sh"]