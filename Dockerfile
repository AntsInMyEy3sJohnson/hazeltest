# Slightly adapted version of https://github.com/docker-library/golang/issues/209#issuecomment-530591780
FROM golang:1.22.3 as builder

WORKDIR /app

COPY . .

# To be filled by build tools such as buildx
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o main .

FROM alpine:3.19.1

WORKDIR /root

COPY --from=builder /app/main .

ENTRYPOINT [ "./main" ]

