FROM golang:1.19 as builder

WORKDIR /src/litefs
COPY ../. .

ARG LITEFS_VERSION=
ARG LITEFS_COMMIT=

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITEFS_VERSION}' -X 'main.Commit=${LITEFS_COMMIT}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litefs ./cmd/litefs

FROM debian:buster-20221219-slim
RUN set -ex \
  && apt-get update \
  && apt-get upgrade -y --no-install-recommends \
  && apt-get install -y curl ca-certificates unzip \
  && apt-get install -y build-essential tcl tcl-dev zlib1g-dev \
  && apt-get install -y kmod procps nano \
  && apt-get install -y fuse 

RUN adduser --disabled-password --gecos "" build

COPY --from=builder /usr/local/bin/litefs /usr/local/bin/litefs
COPY --from=builder /src/litefs/sqlite_tests/litefs.yml /etc/litefs.yml
COPY --from=builder /src/litefs/sqlite_tests/test.sh /usr/bin/test.sh

RUN chmod +x /usr/bin/test.sh

USER build

WORKDIR /home/build

# TODO: Parameterize sqlite version
RUN curl -fsSLO --compressed --create-dirs "https://sqlite.org/2022/sqlite-src-3400100.zip"

# TODO: Unzip to directory without version
RUN unzip sqlite-src-3400100.zip

RUN cd sqlite-src-3400100 && ./configure && make testfixture

ENTRYPOINT ["/usr/bin/test.sh"]
CMD ["extraquick.test"]

# TODO: Create a third stage to docker build that copies binaries
# and test files only to reduce size
