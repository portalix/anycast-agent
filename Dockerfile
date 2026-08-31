FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /anycast-agent ./cmd/anycast-agent

FROM alpine:3.20
COPY --from=build /anycast-agent /usr/local/bin/anycast-agent
ENTRYPOINT ["/usr/local/bin/anycast-agent"]
CMD ["-config", "/etc/anycast-agent/config.yaml"]
