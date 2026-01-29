# Licenses list

Dependencies sometimes change licenses between versions,
please keep this up to date with every new library use.

# Native deps _used_ in production (exclude all test dependencies)

| software        | license                            |
| :----------     | :------------                      |
| abseil          | Apache License 2                   |
| ada             | Apache License 2 / MIT             |
| avro            | Apache License 2                   |
| base64          | BSD 2                              |
| boost libraries | Boost Software License Version 1.0 |
| c-ares          | MIT                                |
| CRoaring        | Apache License 2                   |
| clang           | Apache License 2                   |
| crc32c          | BSD 3                              |
| fmt             | BSD                                |
| HdrHistogram    | BSD 2                              |
| hwloc           | BSD                                |
| jsoncons        | Boost Software License Version 1.0 |
| krb5            | MIT                                |
| lexy            | Boost Software License Version 1.0 |
| libcxx          | Apache License 2                   |
| libcxxabi       | Apache License 2                   |
| libpciaccess    | MIT                                |
| libxml2         | MIT                                |
| liburing        | MIT                                |
| lksctp-tools    | LGPL v2.1                          |
| lz4             | BSD 2                              |
| OpenSSL v3      | Apache License 2                   |
| protobuf        | Apache License 2                   |
| rapidjson       | MIT                                |
| re2             | BSD 3-Clause                       |
| seastar         | Apache License 2                   |
| snappy          | <https://github.com/google/snappy/blob/master/COPYING> |
| unordered_dense | MIT                                |
| xml2            | MIT                                |
| xxhash          | BSD                                |
| xz:liblzma      | Public Domain                      |
| yaml-cpp        | MIT                                |
| zlib            | Zlib                               |
| zstd            | BSD                                |

<!-- 
These are dependencies of wasmtime and can be generated via a script like:

```bash
# Make sure you run this in bazel/thirdparty to pick up our rust dependencies.
rm -f /tmp/license.md
for target in x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
do
    cargo license --avoid-build-deps --avoid-dev-deps --filter-platform=$target --json \
      | jq 'map({name, license})' | jq -r '(.[0] | keys_unsorted) as $keys | map([.[ $keys[] ]])[] | @text "| \(.[0]) | \(.[1]) |"' \
      >> /tmp/license.md
done
cat /tmp/license.md | sort | uniq
```
-->

| rust crate  | license       |
| :---------- | :------------ |
| addr2line | Apache-2.0 OR MIT |
| ahash | Apache-2.0 OR MIT |
| anyhow | Apache-2.0 OR MIT |
| arbitrary | Apache-2.0 OR MIT |
| async-trait | Apache-2.0 OR MIT |
| bitflags | Apache-2.0 OR MIT |
| bumpalo | Apache-2.0 OR MIT |
| cfg-if | Apache-2.0 OR MIT |
| cobs | Apache-2.0 OR MIT |
| cranelift-bforest | Apache-2.0 WITH LLVM-exception |
| cranelift-bitset | Apache-2.0 WITH LLVM-exception |
| cranelift-codegen | Apache-2.0 WITH LLVM-exception |
| cranelift-codegen-shared | Apache-2.0 WITH LLVM-exception |
| cranelift-control | Apache-2.0 WITH LLVM-exception |
| cranelift-entity | Apache-2.0 WITH LLVM-exception |
| cranelift-frontend | Apache-2.0 WITH LLVM-exception |
| cranelift-native | Apache-2.0 WITH LLVM-exception |
| cranelift-wasm | Apache-2.0 WITH LLVM-exception |
| crc32fast | Apache-2.0 OR MIT |
| either | Apache-2.0 OR MIT |
| embedded-io | Apache-2.0 OR MIT |
| equivalent | Apache-2.0 OR MIT |
| errno | Apache-2.0 OR MIT |
| fallible-iterator | Apache-2.0 OR MIT |
| futures | Apache-2.0 OR MIT |
| futures-channel | Apache-2.0 OR MIT |
| futures-core | Apache-2.0 OR MIT |
| futures-io | Apache-2.0 OR MIT |
| futures-sink | Apache-2.0 OR MIT |
| futures-task | Apache-2.0 OR MIT |
| futures-util | Apache-2.0 OR MIT |
| gimli | Apache-2.0 OR MIT |
| hashbrown | Apache-2.0 OR MIT |
| heck | Apache-2.0 OR MIT |
| id-arena | Apache-2.0 OR MIT |
| indexmap | Apache-2.0 OR MIT |
| itertools | Apache-2.0 OR MIT |
| itoa | Apache-2.0 OR MIT |
| leb128 | Apache-2.0 OR MIT |
| libc | Apache-2.0 OR MIT |
| libm | Apache-2.0 OR MIT |
| linux-raw-sys | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| log | Apache-2.0 OR MIT |
| memchr | MIT OR Unlicense |
| memfd | Apache-2.0 OR MIT |
| object | Apache-2.0 OR MIT |
| once_cell | Apache-2.0 OR MIT |
| paste | Apache-2.0 OR MIT |
| pin-project-lite | Apache-2.0 OR MIT |
| pin-utils | Apache-2.0 OR MIT |
| postcard | Apache-2.0 OR MIT |
| proc-macro2 | Apache-2.0 OR MIT |
| quote | Apache-2.0 OR MIT |
| regalloc2 | Apache-2.0 WITH LLVM-exception |
| rustc-hash | Apache-2.0 OR MIT |
| rustix | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| ryu | Apache-2.0 OR BSL-1.0 |
| semver | Apache-2.0 OR MIT |
| serde | Apache-2.0 OR MIT |
| serde_derive | Apache-2.0 OR MIT |
| serde_json | Apache-2.0 OR MIT |
| slice-group-by | MIT |
| smallvec | Apache-2.0 OR MIT |
| sptr | Apache-2.0 OR MIT |
| stable_deref_trait | Apache-2.0 OR MIT |
| syn | Apache-2.0 OR MIT |
| target-lexicon | Apache-2.0 WITH LLVM-exception |
| termcolor | MIT OR Unlicense |
| thiserror | Apache-2.0 OR MIT |
| thiserror-impl | Apache-2.0 OR MIT |
| tracing-attributes | MIT |
| tracing-core | MIT |
| tracing | MIT |
| unicode-ident | (MIT OR Apache-2.0) AND Unicode-DFS-2016 |
| unicode-width | Apache-2.0 OR MIT |
| unicode-xid | Apache-2.0 OR MIT |
| wasm-encoder | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| wasmparser | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| wasmprinter | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| wasmtime | Apache-2.0 WITH LLVM-exception |
| wasmtime-asm-macros | Apache-2.0 WITH LLVM-exception |
| wasmtime-c-api-impl | Apache-2.0 WITH LLVM-exception |
| wasmtime-c-api-macros | Apache-2.0 WITH LLVM-exception |
| wasmtime-component-macro | Apache-2.0 WITH LLVM-exception |
| wasmtime-component-util | Apache-2.0 WITH LLVM-exception |
| wasmtime-cranelift | Apache-2.0 WITH LLVM-exception |
| wasmtime-environ | Apache-2.0 WITH LLVM-exception |
| wasmtime-fiber | Apache-2.0 WITH LLVM-exception |
| wasmtime-jit-icache-coherence | Apache-2.0 WITH LLVM-exception |
| wasmtime-slab | Apache-2.0 WITH LLVM-exception |
| wasmtime-types | Apache-2.0 WITH LLVM-exception |
| wasmtime-versioned-export-macros | Apache-2.0 WITH LLVM-exception |
| wasmtime-wit-bindgen | Apache-2.0 WITH LLVM-exception |
| wast | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| wat | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| wit-parser | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| zerocopy | Apache-2.0 OR BSD-2-Clause OR MIT |

<!--

This list can be auto generated with go-licenses

go install github.com/google/go-licenses@latest

From the RPK directory

go-licenses report ./... --template ../../../licenses/golang_deps.tpl

-->

# Go deps _used_ in production in RPK (exclude all test dependencies)

| software     | license        |
| :----------: | :------------: |
| buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate | [Apache-2.0](https://github.com/bufbuild/protovalidate/blob/main/LICENSE) |
| buf.build/gen/go/grpc-ecosystem/grpc-gateway/protocolbuffers/go/protoc-gen-openapiv2/options | [BSD-3-Clause](https://github.com/grpc-ecosystem/grpc-gateway/blob/main/LICENSE) |
| cloud.google.com/go/compute/metadata | [Apache-2.0](https://github.com/googleapis/google-cloud-go/blob/compute/metadata/v0.9.0/compute/metadata/LICENSE) |
| connectrpc.com/connect | [Apache-2.0](https://github.com/connectrpc/connect-go/blob/v1.19.1/LICENSE) |
| github.com/AlecAivazis/survey/v2 | [MIT](https://github.com/AlecAivazis/survey/blob/v2.3.7/LICENSE) |
| github.com/AlecAivazis/survey/v2/terminal | [MIT](https://github.com/AlecAivazis/survey/blob/v2.3.7/terminal/LICENSE.txt) |
| github.com/Ladicle/tabwriter | [BSD-3-Clause](https://github.com/Ladicle/tabwriter/blob/v1.0.0/LICENSE) |
| github.com/avast/retry-go | [MIT](https://github.com/avast/retry-go/blob/v3.0.0/LICENSE) |
| github.com/aws/aws-sdk-go | [Apache-2.0](https://github.com/aws/aws-sdk-go/blob/v1.55.6/LICENSE.txt) |
| github.com/aws/aws-sdk-go/internal/sync/singleflight | [BSD-3-Clause](https://github.com/aws/aws-sdk-go/blob/v1.55.6/internal/sync/singleflight/LICENSE) |
| github.com/bahlo/generic-list-go | [BSD-3-Clause](https://github.com/bahlo/generic-list-go/blob/v0.2.0/LICENSE) |
| github.com/briandowns/spinner | [Apache-2.0](https://github.com/briandowns/spinner/blob/v1.23.2/LICENSE) |
| github.com/bufbuild/protocompile | [Apache-2.0](https://github.com/bufbuild/protocompile/blob/v0.14.1/LICENSE) |
| github.com/buger/jsonparser | [MIT](https://github.com/buger/jsonparser/blob/v1.1.1/LICENSE) |
| github.com/cespare/xxhash | [MIT](https://github.com/cespare/xxhash/blob/v1.1.0/LICENSE.txt) |
| github.com/cloudflare/cfssl/scan/crypto/md5 | [BSD-2-Clause](https://github.com/cloudflare/cfssl/blob/v1.6.5/LICENSE) |
| github.com/containerd/errdefs | [Apache-2.0](https://github.com/containerd/errdefs/blob/v1.0.0/LICENSE) |
| github.com/containerd/errdefs/pkg | [Apache-2.0](https://github.com/containerd/errdefs/blob/pkg/v0.3.0/pkg/LICENSE) |
| github.com/coreos/go-systemd/v22/dbus | [Apache-2.0](https://github.com/coreos/go-systemd/blob/v22.5.0/LICENSE) |
| github.com/distribution/reference | [Apache-2.0](https://github.com/distribution/reference/blob/v0.6.0/LICENSE) |
| github.com/docker/docker | [Apache-2.0](https://github.com/docker/docker/blob/v28.3.3/LICENSE) |
| github.com/docker/go-connections | [Apache-2.0](https://github.com/docker/go-connections/blob/v0.5.0/LICENSE) |
| github.com/docker/go-units | [Apache-2.0](https://github.com/docker/go-units/blob/v0.5.0/LICENSE) |
| github.com/fatih/color | [MIT](https://github.com/fatih/color/blob/v1.18.0/LICENSE.md) |
| github.com/felixge/httpsnoop | [MIT](https://github.com/felixge/httpsnoop/blob/v1.0.4/LICENSE.txt) |
| github.com/go-logr/logr | [Apache-2.0](https://github.com/go-logr/logr/blob/v1.4.3/LICENSE) |
| github.com/go-logr/stdr | [Apache-2.0](https://github.com/go-logr/stdr/blob/v1.2.2/LICENSE) |
| github.com/godbus/dbus/v5 | [BSD-2-Clause](https://github.com/godbus/dbus/blob/v5.0.4/LICENSE) |
| github.com/gogo/protobuf/proto | [BSD-3-Clause](https://github.com/gogo/protobuf/blob/v1.3.2/LICENSE) |
| github.com/golang/snappy | [BSD-3-Clause](https://github.com/golang/snappy/blob/v0.0.4/LICENSE) |
| github.com/google/uuid | [BSD-3-Clause](https://github.com/google/uuid/blob/v1.6.0/LICENSE) |
| github.com/hamba/avro/v2 | [MIT](https://github.com/redpanda-data/go-avro/blob/77b1144dc525/LICENCE) |
| github.com/hashicorp/errwrap | [MPL-2.0](https://github.com/hashicorp/errwrap/blob/v1.1.0/LICENSE) |
| github.com/hashicorp/go-multierror | [MPL-2.0](https://github.com/hashicorp/go-multierror/blob/v1.1.1/LICENSE) |
| github.com/invopop/jsonschema | [MIT](https://github.com/invopop/jsonschema/blob/v0.13.0/COPYING) |
| github.com/jmespath/go-jmespath | [Apache-2.0](https://github.com/jmespath/go-jmespath/blob/v0.4.0/LICENSE) |
| github.com/json-iterator/go | [MIT](https://github.com/json-iterator/go/blob/v1.1.12/LICENSE) |
| github.com/kballard/go-shellquote | [MIT](https://github.com/kballard/go-shellquote/blob/95032a82bc51/LICENSE) |
| github.com/klauspost/compress | [Apache-2.0](https://github.com/klauspost/compress/blob/v1.18.1/LICENSE) |
| github.com/klauspost/compress/internal/snapref | [BSD-3-Clause](https://github.com/klauspost/compress/blob/v1.18.1/internal/snapref/LICENSE) |
| github.com/klauspost/compress/s2 | [BSD-3-Clause](https://github.com/klauspost/compress/blob/v1.18.1/s2/LICENSE) |
| github.com/klauspost/compress/zstd/internal/xxhash | [MIT](https://github.com/klauspost/compress/blob/v1.18.1/zstd/internal/xxhash/LICENSE.txt) |
| github.com/kr/text | [MIT](https://github.com/kr/text/blob/v0.2.0/License) |
| github.com/lestrrat-go/blackmagic | [MIT](https://github.com/lestrrat-go/blackmagic/blob/v1.0.3/LICENSE) |
| github.com/lestrrat-go/httpcc | [MIT](https://github.com/lestrrat-go/httpcc/blob/v1.0.1/LICENSE) |
| github.com/lestrrat-go/httprc | [MIT](https://github.com/lestrrat-go/httprc/blob/v1.0.6/LICENSE) |
| github.com/lestrrat-go/iter | [MIT](https://github.com/lestrrat-go/iter/blob/v1.0.2/LICENSE) |
| github.com/lestrrat-go/jwx/v2 | [MIT](https://github.com/lestrrat-go/jwx/blob/v2.1.6/LICENSE) |
| github.com/lestrrat-go/option | [MIT](https://github.com/lestrrat-go/option/blob/v1.0.1/LICENSE) |
| github.com/linkedin/goavro/v2 | [Apache-2.0](https://github.com/linkedin/goavro/blob/v2.14.1/LICENSE) |
| github.com/lithammer/go-jump-consistent-hash | [MIT](https://github.com/lithammer/go-jump-consistent-hash/blob/v1.0.2/LICENSE) |
| github.com/mailru/easyjson | [MIT](https://github.com/mailru/easyjson/blob/v0.7.7/LICENSE) |
| github.com/mark3labs/mcp-go | [MIT](https://github.com/mark3labs/mcp-go/blob/v0.37.0/LICENSE) |
| github.com/mattn/go-colorable | [MIT](https://github.com/mattn/go-colorable/blob/v0.1.13/LICENSE) |
| github.com/mattn/go-isatty | [MIT](https://github.com/mattn/go-isatty/blob/v0.0.20/LICENSE) |
| github.com/mgutz/ansi | [MIT](https://github.com/mgutz/ansi/blob/9520e82c474b/LICENSE) |
| github.com/mitchellh/colorstring | [MIT](https://github.com/mitchellh/colorstring/blob/d06e56a500db/LICENSE) |
| github.com/mitchellh/mapstructure | [MIT](https://github.com/mitchellh/mapstructure/blob/v1.5.0/LICENSE) |
| github.com/moby/docker-image-spec/specs-go/v1 | [Apache-2.0](https://github.com/moby/docker-image-spec/blob/v1.3.1/LICENSE) |
| github.com/moby/term | [Apache-2.0](https://github.com/moby/term/blob/v0.5.2/LICENSE) |
| github.com/modern-go/concurrent | [Apache-2.0](https://github.com/modern-go/concurrent/blob/bacd9c7ef1dd/LICENSE) |
| github.com/modern-go/reflect2 | [Apache-2.0](https://github.com/modern-go/reflect2/blob/v1.0.2/LICENSE) |
| github.com/munnerz/goautoneg | [BSD-3-Clause](https://github.com/munnerz/goautoneg/blob/a7dc8b61c822/LICENSE) |
| github.com/opencontainers/go-digest | [Apache-2.0](https://github.com/opencontainers/go-digest/blob/v1.0.0/LICENSE) |
| github.com/opencontainers/image-spec/specs-go | [Apache-2.0](https://github.com/opencontainers/image-spec/blob/v1.1.1/LICENSE) |
| github.com/pierrec/lz4/v4 | [BSD-3-Clause](https://github.com/pierrec/lz4/blob/v4.1.22/LICENSE) |
| github.com/pkg/browser | [BSD-2-Clause](https://github.com/pkg/browser/blob/5ac0b6a4141c/LICENSE) |
| github.com/pkg/errors | [BSD-2-Clause](https://github.com/pkg/errors/blob/v0.9.1/LICENSE) |
| github.com/prometheus/client_model/go | [Apache-2.0](https://github.com/prometheus/client_model/blob/v0.6.2/LICENSE) |
| github.com/prometheus/common | [Apache-2.0](https://github.com/prometheus/common/blob/v0.65.0/LICENSE) |
| github.com/rivo/uniseg | [MIT](https://github.com/rivo/uniseg/blob/v0.4.7/LICENSE.txt) |
| github.com/rs/xid | [MIT](https://github.com/rs/xid/blob/v1.6.0/LICENSE) |
| github.com/santhosh-tekuri/jsonschema/v6 | [Apache-2.0](https://github.com/santhosh-tekuri/jsonschema/blob/v6.0.2/LICENSE) |
| github.com/schollz/progressbar/v3 | [MIT](https://github.com/schollz/progressbar/blob/v3.18.0/LICENSE) |
| github.com/sethgrid/pester | [MIT](https://github.com/sethgrid/pester/blob/v1.2.0/LICENSE.md) |
| github.com/spf13/afero | [Apache-2.0](https://github.com/spf13/afero/blob/v1.15.0/LICENSE.txt) |
| github.com/spf13/cast | [MIT](https://github.com/spf13/cast/blob/v1.7.1/LICENSE) |
| github.com/spf13/cobra | [Apache-2.0](https://github.com/spf13/cobra/blob/v1.10.1/LICENSE.txt) |
| github.com/spf13/pflag | [BSD-3-Clause](https://github.com/spf13/pflag/blob/v1.0.10/LICENSE) |
| github.com/tidwall/gjson | [MIT](https://github.com/tidwall/gjson/blob/v1.14.4/LICENSE) |
| github.com/tidwall/match | [MIT](https://github.com/tidwall/match/blob/v1.1.1/LICENSE) |
| github.com/tidwall/pretty | [MIT](https://github.com/tidwall/pretty/blob/v1.2.1/LICENSE) |
| github.com/tidwall/sjson | [MIT](https://github.com/tidwall/sjson/blob/v1.2.5/LICENSE) |
| github.com/tklauser/go-sysconf | [BSD-3-Clause](https://github.com/tklauser/go-sysconf/blob/v0.3.15/LICENSE) |
| github.com/twmb/franz-go/pkg | [BSD-3-Clause](https://github.com/twmb/franz-go/blob/v1.20.4/LICENSE) |
| github.com/twmb/franz-go/pkg/kadm | [BSD-3-Clause](https://github.com/twmb/franz-go/blob/pkg/kadm/v1.17.1/pkg/kadm/LICENSE) |
| github.com/twmb/franz-go/pkg/kmsg | [BSD-3-Clause](https://github.com/twmb/franz-go/blob/pkg/kmsg/v1.12.0/pkg/kmsg/LICENSE) |
| github.com/twmb/franz-go/pkg/sr | [BSD-3-Clause](https://github.com/twmb/franz-go/blob/pkg/sr/v1.5.0/pkg/sr/LICENSE) |
| github.com/twmb/franz-go/plugin/kzap | [BSD-3-Clause](https://github.com/twmb/franz-go/blob/plugin/kzap/v1.1.2/plugin/kzap/LICENSE) |
| github.com/twmb/tlscfg | [BSD-3-Clause](https://github.com/twmb/tlscfg/blob/v1.2.1/LICENSE) |
| github.com/twmb/types | [BSD-3-Clause](https://github.com/twmb/types/blob/v1.1.6/LICENSE) |
| github.com/wk8/go-ordered-map/v2 | [Apache-2.0](https://github.com/wk8/go-ordered-map/blob/v2.1.8/LICENSE) |
| github.com/yosida95/uritemplate/v3 | [BSD-3-Clause](https://github.com/yosida95/uritemplate/blob/v3.0.2/LICENSE) |
| go.opentelemetry.io/auto/sdk | [Apache-2.0](https://github.com/open-telemetry/opentelemetry-go-instrumentation/blob/sdk/v1.1.0/sdk/LICENSE) |
| go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp | [Apache-2.0](https://github.com/open-telemetry/opentelemetry-go-contrib/blob/instrumentation/net/http/otelhttp/v0.62.0/instrumentation/net/http/otelhttp/LICENSE) |
| go.opentelemetry.io/otel | [Apache-2.0](https://github.com/open-telemetry/opentelemetry-go/blob/v1.37.0/LICENSE) |
| go.opentelemetry.io/otel/metric | [Apache-2.0](https://github.com/open-telemetry/opentelemetry-go/blob/metric/v1.37.0/metric/LICENSE) |
| go.opentelemetry.io/otel/trace | [Apache-2.0](https://github.com/open-telemetry/opentelemetry-go/blob/trace/v1.37.0/trace/LICENSE) |
| go.uber.org/multierr | [MIT](https://github.com/uber-go/multierr/blob/v1.11.0/LICENSE.txt) |
| go.uber.org/zap | [MIT](https://github.com/uber-go/zap/blob/v1.27.0/LICENSE) |
| golang.org/x/crypto | [BSD-3-Clause](https://cs.opensource.google/go/x/crypto/+/v0.43.0:LICENSE) |
| golang.org/x/exp/maps | [BSD-3-Clause](https://cs.opensource.google/go/x/exp/+/a4bb9ffd:LICENSE) |
| golang.org/x/net | [BSD-3-Clause](https://cs.opensource.google/go/x/net/+/v0.46.0:LICENSE) |
| golang.org/x/sync | [BSD-3-Clause](https://cs.opensource.google/go/x/sync/+/v0.17.0:LICENSE) |
| golang.org/x/sys/unix | [BSD-3-Clause](https://cs.opensource.google/go/x/sys/+/v0.37.0:LICENSE) |
| golang.org/x/term | [BSD-3-Clause](https://cs.opensource.google/go/x/term/+/v0.36.0:LICENSE) |
| golang.org/x/text | [BSD-3-Clause](https://cs.opensource.google/go/x/text/+/v0.30.0:LICENSE) |
| google.golang.org/genproto/googleapis/api | [Apache-2.0](https://github.com/googleapis/go-genproto/blob/f26f9409b101/googleapis/api/LICENSE) |
| google.golang.org/genproto/googleapis/rpc | [Apache-2.0](https://github.com/googleapis/go-genproto/blob/ab9386a59fda/googleapis/rpc/LICENSE) |
| google.golang.org/genproto/googleapis/type | [Apache-2.0](https://github.com/googleapis/go-genproto/blob/de1ac958c67a/LICENSE) |
| google.golang.org/grpc | [Apache-2.0](https://github.com/grpc/grpc-go/blob/v1.73.0/LICENSE) |
| google.golang.org/protobuf | [BSD-3-Clause](https://github.com/protocolbuffers/protobuf-go/blob/v1.36.11/LICENSE) |
| gopkg.in/yaml.v2 | [Apache-2.0](https://github.com/go-yaml/yaml/blob/v2.4.0/LICENSE) |
| gopkg.in/yaml.v3 | [MIT](https://github.com/go-yaml/yaml/blob/v3.0.1/LICENSE) |
| k8s.io/apimachinery/pkg | [Apache-2.0](https://github.com/kubernetes/apimachinery/blob/v0.33.3/LICENSE) |
| k8s.io/klog/v2 | [Apache-2.0](https://github.com/kubernetes/klog/blob/v2.130.1/LICENSE) |
| k8s.io/utils/internal/third_party/forked/golang/net | [BSD-3-Clause](https://github.com/kubernetes/utils/blob/3ea5e8cea738/internal/third_party/forked/golang/LICENSE) |
| k8s.io/utils/net | [Apache-2.0](https://github.com/kubernetes/utils/blob/3ea5e8cea738/LICENSE) |

