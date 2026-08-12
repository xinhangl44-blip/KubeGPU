FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gpu-exporter ./cmd/gpu-usage-exporter

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/gpu-exporter .
RUN apk add --no-cache libc6-compat
ENTRYPOINT ["./gpu-exporter"]
