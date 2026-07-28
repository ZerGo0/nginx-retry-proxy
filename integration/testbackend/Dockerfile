FROM golang:1.26-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build
WORKDIR /src
COPY go.mod ./
COPY integration/testbackend ./integration/testbackend
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/testbackend ./integration/testbackend

FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/testbackend /usr/local/bin/testbackend
USER app
ENTRYPOINT ["/usr/local/bin/testbackend"]
