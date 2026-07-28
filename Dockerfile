FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/nginx-retry-proxy ./cmd/nginx-retry-proxy

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 proxy \
    && adduser -S -D -H -u 10001 -G proxy proxy
COPY --from=build /out/nginx-retry-proxy /usr/local/bin/nginx-retry-proxy
USER proxy
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/nginx-retry-proxy"]
