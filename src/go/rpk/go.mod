module github.com/redpanda-data/redpanda/src/go/rpk

go 1.24.3

// add the git commit hash as the target version and `go mod tidy` will transform it into pseudo-version
replace github.com/hamba/avro/v2 => github.com/redpanda-data/go-avro/v2 v2.0.0-20240405204525-77b1144dc525

require (
	buf.build/gen/go/redpandadata/cloud/connectrpc/go v1.18.1-20250806153840-184e270a51f5.1
	buf.build/gen/go/redpandadata/cloud/protocolbuffers/go v1.36.7-20250806153840-184e270a51f5.1
	buf.build/gen/go/redpandadata/common/protocolbuffers/go v1.36.7-20250718021421-04f2daa29ad9.1
	buf.build/gen/go/redpandadata/core/protocolbuffers/go v1.36.10-20250930194408-070fbb41a3d0.1
	buf.build/gen/go/redpandadata/dataplane/connectrpc/go v1.18.1-20250819145731-621dc775ffe4.1
	buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go v1.36.7-20250819145731-621dc775ffe4.1
	buf.build/gen/go/redpandadata/gatekeeper/connectrpc/go v1.18.1-20250813192242-efcc93bcd794.1
	buf.build/gen/go/redpandadata/gatekeeper/protocolbuffers/go v1.36.7-20250813192242-efcc93bcd794.1
	cloud.google.com/go/compute/metadata v0.7.0
	connectrpc.com/connect v1.19.0
	github.com/AlecAivazis/survey/v2 v2.3.7
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/aws/aws-sdk-go v1.55.6
	github.com/beevik/ntp v1.4.3
	github.com/bufbuild/protocompile v0.14.1
	github.com/cespare/xxhash v1.1.0
	github.com/containerd/errdefs v1.0.0
	github.com/coreos/go-systemd/v22 v22.5.0
	github.com/docker/docker v28.3.3+incompatible
	github.com/docker/go-connections v0.5.0
	github.com/docker/go-units v0.5.0
	github.com/fatih/color v1.18.0
	github.com/google/uuid v1.6.0
	github.com/hamba/avro/v2 v2.29.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/kr/text v0.2.0
	github.com/lestrrat-go/jwx/v2 v2.1.6
	github.com/linkedin/goavro/v2 v2.14.0
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/lorenzosaino/go-sysctl v0.3.1
	github.com/mark3labs/mcp-go v0.37.0
	github.com/mattn/go-isatty v0.0.20
	github.com/moby/term v0.5.2
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.1
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.65.0
	github.com/redpanda-data/common-go/proto v0.0.0-20250820120127-9b518fca5ecf
	github.com/redpanda-data/common-go/rpadmin v0.1.17-0.20251001175055-902a29f7fa07
	github.com/redpanda-data/common-go/rpsr v0.1.2
	github.com/redpanda-data/protoc-gen-go-mcp v0.0.0-20250812151819-7e5d5fef8241
	github.com/rs/xid v1.6.0
	github.com/safchain/ethtool v0.6.2
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2
	github.com/schollz/progressbar/v3 v3.18.0
	github.com/spf13/afero v1.14.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/pflag v1.0.7
	github.com/stretchr/testify v1.10.0
	github.com/tidwall/sjson v1.2.5
	github.com/tklauser/go-sysconf v0.3.15
	github.com/twmb/franz-go v1.19.5
	github.com/twmb/franz-go/pkg/kadm v1.16.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20250729165834-29dc44e616cd
	github.com/twmb/franz-go/pkg/kmsg v1.11.2
	github.com/twmb/franz-go/pkg/sr v1.5.0
	github.com/twmb/franz-go/plugin/kzap v1.1.2
	github.com/twmb/tlscfg v1.2.1
	github.com/twmb/types v1.1.6
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20250718183923-645b1fa84792
	golang.org/x/sync v0.16.0
	golang.org/x/sys v0.35.0
	golang.org/x/term v0.34.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250922171735-9219d122eba9
	google.golang.org/protobuf v1.36.10
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.33.3
	k8s.io/apimachinery v0.33.3
	k8s.io/client-go v0.33.3
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.7-20250613105001-9f2d3c737feb.1 // indirect
	buf.build/gen/go/grpc-ecosystem/grpc-gateway/protocolbuffers/go v1.36.7-20240617172850-a48fcebcf8f1.1 // indirect
	buf.build/gen/go/redpandadata/core/connectrpc/go v1.19.0-20250930194408-070fbb41a3d0.1 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/BurntSushi/toml v1.1.0 // indirect
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/cloudflare/cfssl v1.6.5 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/godbus/dbus/v5 v5.0.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/gnostic-models v0.6.9 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/invopop/jsonschema v0.13.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/lestrrat-go/blackmagic v1.0.3 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/httprc v1.0.6 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/sys/atomicwriter v0.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/redpanda-data/common-go/api v0.0.0-20250801174835-9eea07f1ea06 // indirect
	github.com/redpanda-data/common-go/net v0.1.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/sethgrid/pester v1.2.0 // indirect
	github.com/spf13/cast v1.7.1 // indirect
	github.com/tidwall/gjson v1.14.4 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.62.0 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.37.0 // indirect
	go.opentelemetry.io/otel/metric v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20220613132600-b0d781184e0d // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.27.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/oauth2 v0.30.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	golang.org/x/time v0.9.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	google.golang.org/genproto v0.0.0-20250409194420-de1ac958c67a // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251002232023-7c0ddcbb5797 // indirect
	google.golang.org/grpc v1.73.0 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.12.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gotest.tools/v3 v3.5.2 // indirect
	honnef.co/go/tools v0.3.2 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/kube-openapi v0.0.0-20250318190949-c8a335a9a2ff // indirect
	k8s.io/utils v0.0.0-20241104100929-3ea5e8cea738 // indirect
	sigs.k8s.io/json v0.0.0-20241010143419-9aa6b5e7a4b3 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.6.0 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)
