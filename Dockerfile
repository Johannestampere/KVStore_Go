FROM golang:1.27.1-alpine3.24 AS build

WORKDIR /src
COPY go.mod ./
COPY cmd/node/ ./cmd/node/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 go build -trimpath -o /out/node ./cmd/node

FROM alpine:3.24

RUN addgroup -S kvstore && adduser -S -G kvstore kvstore \
    && mkdir -p /var/lib/kvstore \
    && chown kvstore:kvstore /var/lib/kvstore
COPY --from=build /out/node /usr/local/bin/node
COPY configs/cluster.docker.json /etc/kvstore/cluster.json

USER kvstore
EXPOSE 8000
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/node"]
CMD ["-addr", "0.0.0.0:8000", "-data-dir", "/var/lib/kvstore"]
