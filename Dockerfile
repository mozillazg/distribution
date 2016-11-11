FROM golang:1.6-alpine

ENV DISTRIBUTION_DIR /go/src/github.com/docker/distribution
ENV DOCKER_BUILDTAGS include_qiniu

RUN set -ex \
    && apk add --no-cache make git
RUN go get github.com/getsentry/raven-go


WORKDIR $DISTRIBUTION_DIR
COPY . $DISTRIBUTION_DIR
COPY cmd/registry/config-dev.yml /etc/docker/registry/config.yml

# RUN make PREFIX=/go clean binaries
WORKDIR $DISTRIBUTION_DIR/cmd/registry
RUN go build -tags "include_qiniu noresumabledigest"

VOLUME ["/var/lib/registry"]
EXPOSE 5000
ENTRYPOINT ["./registry"]
CMD ["serve", "/etc/docker/registry/config.yml"]
