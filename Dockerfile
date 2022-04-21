FROM golang:1.17 as builder

ARG PASSWORD
ENV GOPRIVATE=github.com/liteserver/*

RUN echo "machine github.com login liteserver password ${PASSWORD}" > ~/.netrc && chmod 600 ~/.netrc

COPY . /src/litefs
WORKDIR /src/litefs

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags '-s -w -extldflags "-static"' -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litefs ./cmd/litefs

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags '-s -w -extldflags "-static"' -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litefs-demo ./cmd/litefs-demo


FROM alpine

COPY --from=builder /usr/local/bin/litefs /usr/local/bin/litefs
COPY --from=builder /usr/local/bin/litefs-demo /usr/local/bin/litefs-demo

ADD run.sh /run.sh
RUN apk update -U && apk add bash ca-certificates curl fuse sqlite
RUN mkdir -p /data

CMD /run.sh
