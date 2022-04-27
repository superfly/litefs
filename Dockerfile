FROM golang:1.17 as builder

ARG PASSWORD
ENV GOPRIVATE=github.com/superfly/*

RUN echo "machine github.com login benbjohnson password ${PASSWORD}" > ~/.netrc && chmod 600 ~/.netrc

COPY . /src/litefs
WORKDIR /src/litefs

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags '-s -w -extldflags "-static"' -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litefs ./cmd/litefs


FROM alpine

COPY --from=builder /usr/local/bin/litefs /usr/local/bin/litefs

ADD run.sh /run.sh
RUN apk update -U && apk add bash ca-certificates curl fuse sqlite
RUN mkdir -p /data

CMD /run.sh
