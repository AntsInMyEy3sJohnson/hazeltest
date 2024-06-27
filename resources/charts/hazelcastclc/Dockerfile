FROM bash:4.0.44-alpine3.20

RUN apk update && \
    apk add --no-cache curl && \
    curl https://hazelcast.com/clc/install.sh | bash

# Sample command for creating multi-arch image:
# docker buildx build --push --platform linux/amd64,linux/arm64 --tag antsinmyey3sjohnson/hazelcastclc:5.4.0 --tag antsinmyey3sjohnson/hazelcastclc:latest .
ENTRYPOINT ["bash", "-c"]