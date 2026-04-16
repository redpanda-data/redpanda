module github.com/redpanda-data/redpanda/src/go/rpk

go 1.26.2

require (
	buf.build/gen/go/redpandadata/ai-gateway/connectrpc/go v1.19.1-20260203101113-1c7702ddb57a.2
	buf.build/gen/go/redpandadata/cloud/connectrpc/go v1.19.1-20260409133226-cd0bdc42a0f9.2
	buf.build/gen/go/redpandadata/cloud/protocolbuffers/go v1.36.11-20260409133226-cd0bdc42a0f9.1
	buf.build/gen/go/redpandadata/common/protocolbuffers/go v1.36.11-20260323171043-6e06f84ad823.1
	buf.build/gen/go/redpandadata/core/protocolbuffers/go v1.36.11-20260409143030-36db7bac51c2.1
	buf.build/gen/go/redpandadata/dataplane/connectrpc/go v1.19.1-20260409091301-5e03d1b04513.2
	buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go v1.36.11-20260409091301-5e03d1b04513.1
	buf.build/gen/go/redpandadata/gatekeeper/connectrpc/go v1.19.1-20251022210437-a5dd600d04b6.2
	buf.build/gen/go/redpandadata/gatekeeper/protocolbuffers/go v1.36.11-20251022210437-a5dd600d04b6.1
	cloud.google.com/go/compute/metadata v0.9.0
	connectrpc.com/connect v1.19.1
	github.com/AlecAivazis/survey/v2 v2.3.7
	github.com/Ladicle/tabwriter v1.0.0
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/aws/aws-sdk-go-v2/config v1.32.14
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.21
	github.com/beevik/ntp v1.5.0
	github.com/briandowns/spinner v1.23.2
	github.com/bufbuild/protocompile v0.14.1
	github.com/cespare/xxhash v1.1.0
	github.com/containerd/errdefs v1.0.0
	github.com/coreos/go-systemd/v22 v22.7.0
	github.com/docker/docker v28.5.2+incompatible
	github.com/docker/go-connections v0.6.0
	github.com/docker/go-units v0.5.0
	github.com/fatih/color v1.19.0
	github.com/google/uuid v1.6.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51
	github.com/kr/text v0.2.0
	github.com/lestrrat-go/jwx/v2 v2.1.6
	github.com/lithammer/go-jump-consistent-hash v1.0.2
	github.com/lorenzosaino/go-sysctl v0.3.1
	github.com/mark3labs/mcp-go v0.47.1
	github.com/mattn/go-isatty v0.0.21
	github.com/moby/term v0.5.2
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.1
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.67.5
	github.com/redpanda-data/common-go/proto v0.0.0-20260223115805-73fb9bd9c2c0
	github.com/redpanda-data/common-go/rpadmin v0.2.5
	github.com/redpanda-data/common-go/rpsr v0.1.4
	github.com/redpanda-data/protoc-gen-go-mcp v0.0.0-20250930092048-a98b94b5957a
	github.com/rs/xid v1.6.0
	github.com/safchain/ethtool v0.7.0
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2
	github.com/schollz/progressbar/v3 v3.19.0
	github.com/spf13/afero v1.15.0
	github.com/spf13/cobra v1.10.2
	github.com/spf13/pflag v1.0.10
	github.com/stretchr/testify v1.11.1
	github.com/tidwall/sjson v1.2.5
	github.com/tklauser/go-sysconf v0.3.16
	github.com/twmb/avro v1.3.4
	github.com/twmb/franz-go v1.20.5
	github.com/twmb/franz-go/pkg/kadm v1.17.2
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20251024215757-aea970d4d0d2
	github.com/twmb/franz-go/pkg/kmsg v1.13.1
	github.com/twmb/franz-go/pkg/sr v1.7.0
	github.com/twmb/franz-go/plugin/kzap v1.1.2
	github.com/twmb/tlscfg v1.3.0
	github.com/twmb/types v1.2.0
	go.uber.org/zap v1.27.1
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90
	golang.org/x/sync v0.20.0
	golang.org/x/sys v0.43.0
	golang.org/x/term v0.42.0
	google.golang.org/genproto/googleapis/api v0.0.0-20260406210006-6f92a3bedf2d
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260406210006-6f92a3bedf2d
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.35.3
	k8s.io/apimachinery v0.35.3
	k8s.io/client-go v0.35.3
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.11-20260209202127-80ab13bee0bf.1 // indirect
	buf.build/gen/go/grpc-ecosystem/grpc-gateway/protocolbuffers/go v1.36.11-20260102203250-6467306b4f62.1 // indirect
	buf.build/gen/go/redpandadata/ai-gateway/protocolbuffers/go v1.36.11-20260203101113-1c7702ddb57a.1 // indirect
	buf.build/gen/go/redpandadata/core/connectrpc/go v1.19.1-20260409143030-36db7bac51c2.2 // indirect
	buf.build/gen/go/redpandadata/otel/protocolbuffers/go v1.36.11-20260323171043-3635d3966b23.1 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/BurntSushi/toml v1.6.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.5 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.14 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.10 // indirect
	github.com/aws/smithy-go v1.24.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.1 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/emicklei/go-restful/v3 v3.13.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fxamacker/cbor/v2 v2.9.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.22.5 // indirect
	github.com/go-openapi/jsonreference v0.21.5 // indirect
	github.com/go-openapi/swag v0.25.5 // indirect
	github.com/go-openapi/swag/cmdutils v0.25.5 // indirect
	github.com/go-openapi/swag/conv v0.25.5 // indirect
	github.com/go-openapi/swag/fileutils v0.25.5 // indirect
	github.com/go-openapi/swag/jsonname v0.25.5 // indirect
	github.com/go-openapi/swag/jsonutils v0.25.5 // indirect
	github.com/go-openapi/swag/loading v0.25.5 // indirect
	github.com/go-openapi/swag/mangling v0.25.5 // indirect
	github.com/go-openapi/swag/netutils v0.25.5 // indirect
	github.com/go-openapi/swag/stringutils v0.25.5 // indirect
	github.com/go-openapi/swag/typeutils v0.25.5 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.5 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/godbus/dbus/v5 v5.2.2 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/lestrrat-go/blackmagic v1.0.4 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/httprc v1.0.6 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/sys/atomicwriter v0.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/redpanda-data/common-go/api v0.0.0-20260409173929-7e154ef990df // indirect
	github.com/redpanda-data/common-go/net v0.1.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/sethgrid/pester v1.2.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.68.0 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.37.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20260312153236-7ab1446f8b90 // indirect
	golang.org/x/lint v0.0.0-20241112194109-818c5a804067 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
	google.golang.org/genproto v0.0.0-20260406210006-6f92a3bedf2d // indirect
	google.golang.org/grpc v1.80.0 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.13.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gotest.tools/v3 v3.5.2 // indirect
	honnef.co/go/tools v0.7.0 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	k8s.io/kube-openapi v0.0.0-20260330154417-16be699c7b31 // indirect
	k8s.io/utils v0.0.0-20260319190234-28399d86e0b5 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.2 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)
