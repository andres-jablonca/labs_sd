FROM golang:1.25-alpine

WORKDIR /app

COPY . .

RUN go mod tidy
RUN go build -o trevor .

EXPOSE 50051

CMD ["./trevor"]