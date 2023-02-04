module github.com/superfly/litefs

go 1.19

require (
	bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5
	github.com/hashicorp/consul/api v1.11.0
	github.com/mattn/go-shellwords v1.0.12
	github.com/mattn/go-sqlite3 v1.14.16-0.20220918133448-90900be5db1a
	github.com/prometheus/client_golang v1.13.0
	github.com/superfly/ltx v0.3.0
	golang.org/x/net v0.0.0-20220909164309-bea034e7d591
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/fatih/color v1.9.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/memberlist v0.3.1 // indirect
	github.com/hashicorp/serf v0.9.7 // indirect
	github.com/mattn/go-colorable v0.1.6 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/go-testing-interface v1.14.0 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.37.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/sys v0.4.0 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

// replace github.com/mattn/go-sqlite3 => ../../mattn/go-sqlite3
// replace github.com/pierrec/lz4/v4 => ../../pierrec/lz4
// replace github.com/superfly/ltx => ../ltx
