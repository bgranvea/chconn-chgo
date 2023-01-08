FROM golang:1.19.4-alpine as builder
WORKDIR /build
RUN mkdir bin
ADD . .
ARG PROXY
RUN HTTPS_PROXY=${PROXY} CGO_ENABLED=0 go build -o ./bin ./chgotest
RUN HTTPS_PROXY=${PROXY} CGO_ENABLED=0 go build -o ./bin ./chconntest
RUN HTTPS_PROXY=${PROXY} CGO_ENABLED=0 go build -o ./bin ./chconn3test

FROM busybox:1.35.0
COPY --from=builder /build/bin/* /
COPY --from=builder /build/*.sh /
