# Slightly adapted version of https://github.com/docker-library/golang/issues/209#issuecomment-530591780
FROM golang:1.18.0 as builder

ENV CGO_ENABLED 0
ENV GOOS linux

WORKDIR /app

COPY . .

RUN go build -o main .

FROM alpine:3.15.4

WORKDIR /root

COPY --from=builder /app/main .

ENTRYPOINT [ "./main" ]

