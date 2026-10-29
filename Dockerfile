# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ledger ./cmd/ledger

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 ledger
COPY --from=build /out/ledger /usr/local/bin/ledger
USER ledger
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ledger"]
CMD ["serve"]
