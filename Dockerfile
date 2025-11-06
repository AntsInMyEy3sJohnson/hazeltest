FROM golang:1.25.3 as builder

WORKDIR /build

COPY . .

ARG APP_EXECUTABLE=main

# To be filled by build tools such as buildx
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o $APP_EXECUTABLE .

# Set file permissions so all users belonging to the 'root' group
# can read and execute the binary
# --> Enables image to be used on OpenShift, which will -- if nothing else is
# specified -- select an arbitrary user to spin up the image in a Pod's container
# See: https://docs.openshift.com/container-platform/4.17/openshift_images/create-images.html#use-uid_create-images
RUN chgrp 0 $APP_EXECUTABLE && chmod g=u $APP_EXECUTABLE

FROM alpine:3.22.2 as runner

ARG APP_DIR=/app

# Set permissions on directory that will contain application executable
# in accordance with requirements for OpenShift, too
# (Hint: In theory, one could copy the executable into the runner image and
# encapsulate setting appropriate permissions in one command after doing so,
# but this would introduce a file system layer into the image effectively
# doubling the image's size, so to prevent that, setting permissions on the
# executable and its enclosing folder was split into the build and runner image)
RUN mkdir $APP_DIR && chgrp 0 $APP_DIR && chmod g=u $APP_DIR
WORKDIR $APP_DIR

COPY --from=builder /build/main .

ENTRYPOINT [ "./main" ]