FROM golang:1.18 as builder

WORKDIR /src/litefs
COPY . .

ARG GITHUB_TOKEN
ARG LITEFS_VERSION=latest

RUN git config --global url."https://${GITHUB_TOKEN}:@github.com/".insteadOf "https://github.com/"

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -tags sqlite_os_trace -ldflags "-s -w -X 'main.Version=${LITEFS_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litefs ./cmd/litefs


FROM scratch
COPY --from=builder /usr/local/bin/litefs /usr/local/bin/litefs
ENTRYPOINT ["/usr/local/bin/litefs"]
CMD []
