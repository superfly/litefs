FROM golang:1.21.5 as builder

WORKDIR /src/litefs
COPY . .

ARG LITEFS_VERSION=
ARG LITEFS_COMMIT=

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITEFS_VERSION}' -X 'main.Commit=${LITEFS_COMMIT}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litefs ./cmd/litefs


FROM scratch
COPY --from=builder /usr/local/bin/litefs /usr/local/bin/litefs
ENTRYPOINT ["/usr/local/bin/litefs"]
CMD []
