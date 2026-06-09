FROM golang:1.22-alpine AS builder
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY go.mod go.sum ./
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=1 go build -o wa-intel .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata sqlite curl
WORKDIR /app
COPY --from=builder /app/wa-intel .
EXPOSE 8090
ENTRYPOINT ["./wa-intel"]
