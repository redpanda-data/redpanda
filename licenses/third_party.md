# Licenses list

Dependencies sometimes change licenses between versions,
please keep this up to date with every new library use.

# Native deps _used_ in production (exclude all test dependencies)

| software        | license                            |
| :----------     | :------------                      |
| abseil          | Apache License 2                   |
| ada             | Apache License 2 / MIT             |
| arrow           | Apache License 2 / MIT / Boost / BSD 2 & 3 clause / ZPL / LLVM / <https://github.com/apache/arrow/blob/main/LICENSE.txt> |
| avro            | Apache License 2                   |
| base64          | BSD 2                              |
| boost libraries | Boost Software License Version 1.0 |
| c-ares          | MIT                                |
| CRoaring        | Apache License 2                   |
| clang           | Apache License 2                   |
| crc32c          | BSD 3                              |
| DPDK            | BSD                                |
| fmt             | BSD                                |
| HdrHistogram    | BSD 2                              |
| hwloc           | BSD                                |
| jsoncons        | Boost Software License Version 1.0 |
| krb5            | MIT                                |
| libcxx          | Apache License 2                   |
| libcxxabi       | Apache License 2                   |
| libnumactl      | LGPL v2.1                          |
| libpciaccess    | MIT                                |
| lksctp-tools    | LGPL v2.1                          |
| lz4             | BSD 2                              |
| OpenSSL v3      | Apache License 2                   |
| protobuf        | Apache License 2                   |
| rapidjson       | MIT                                |
| re2             | BSD 3-Clause                       |
| sasl2           | <https://github.com/cyrusimap/cyrus-sasl/blob/master/COPYING> |
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
# Make sure you run this in a checked out wasmtime repo with the same version that we use.
rm -f /tmp/license.md
for target in x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
do
    cargo license --no-default-features --features=async --features=wat --avoid-build-deps --avoid-dev-deps --filter-platform=$target --json \
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
| base64 | Apache-2.0 OR MIT |
| bincode | MIT |
| bitflags | Apache-2.0 OR MIT |
| block-buffer | Apache-2.0 OR MIT |
| bumpalo | Apache-2.0 OR MIT |
| byteorder | MIT OR Unlicense |
| capstone | MIT |
| capstone-sys | MIT |
| cfg-if | Apache-2.0 OR MIT |
| cpp_demangle | Apache-2.0 OR MIT |
| cpufeatures | Apache-2.0 OR MIT |
| cranelift-bforest | Apache-2.0 WITH LLVM-exception |
| cranelift-codegen | Apache-2.0 WITH LLVM-exception |
| cranelift-codegen-shared | Apache-2.0 WITH LLVM-exception |
| cranelift-control | Apache-2.0 WITH LLVM-exception |
| cranelift-entity | Apache-2.0 WITH LLVM-exception |
| cranelift-frontend | Apache-2.0 WITH LLVM-exception |
| cranelift-native | Apache-2.0 WITH LLVM-exception |
| cranelift-wasm | Apache-2.0 WITH LLVM-exception |
| crc32fast | Apache-2.0 OR MIT |
| crossbeam-deque | Apache-2.0 OR MIT |
| crossbeam-epoch | Apache-2.0 OR MIT |
| crossbeam-utils | Apache-2.0 OR MIT |
| crypto-common | Apache-2.0 OR MIT |
| debugid | Apache-2.0 |
| derive_arbitrary | Apache-2.0 OR MIT |
| digest | Apache-2.0 OR MIT |
| directories-next | Apache-2.0 OR MIT |
| dirs-sys-next | Apache-2.0 OR MIT |
| either | Apache-2.0 OR MIT |
| encoding_rs | (Apache-2.0 OR MIT) AND BSD-3-Clause |
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
| fxhash | Apache-2.0 OR MIT |
| fxprof-processed-profile | Apache-2.0 OR MIT |
| generic-array | MIT |
| getrandom | Apache-2.0 OR MIT |
| gimli | Apache-2.0 OR MIT |
| hashbrown | Apache-2.0 OR MIT |
| heck | Apache-2.0 OR MIT |
| id-arena | Apache-2.0 OR MIT |
| indexmap | Apache-2.0 OR MIT |
| itertools | Apache-2.0 OR MIT |
| itoa | Apache-2.0 OR MIT |
| ittapi | BSD-3-Clause OR GPL-2.0-only |
| ittapi-sys | BSD-3-Clause OR GPL-2.0-only |
| leb128 | Apache-2.0 OR MIT |
| libc | Apache-2.0 OR MIT |
| linux-raw-sys | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| log | Apache-2.0 OR MIT |
| memchr | MIT OR Unlicense |
| memfd | Apache-2.0 OR MIT |
| memoffset | MIT |
| object | Apache-2.0 OR MIT |
| once_cell | Apache-2.0 OR MIT |
| paste | Apache-2.0 OR MIT |
| pin-project-lite | Apache-2.0 OR MIT |
| pin-utils | Apache-2.0 OR MIT |
| proc-macro2 | Apache-2.0 OR MIT |
| quote | Apache-2.0 OR MIT |
| rayon | Apache-2.0 OR MIT |
| rayon-core | Apache-2.0 OR MIT |
| regalloc2 | Apache-2.0 WITH LLVM-exception |
| rustc-demangle | Apache-2.0 OR MIT |
| rustc-hash | Apache-2.0 OR MIT |
| rustix | Apache-2.0 OR Apache-2.0 WITH LLVM-exception OR MIT |
| ryu | Apache-2.0 OR BSL-1.0 |
| scopeguard | Apache-2.0 OR MIT |
| semver | Apache-2.0 OR MIT |
| serde | Apache-2.0 OR MIT |
| serde_derive | Apache-2.0 OR MIT |
| serde_json | Apache-2.0 OR MIT |
| sha2 | Apache-2.0 OR MIT |
| slice-group-by | MIT |
| smallvec | Apache-2.0 OR MIT |
| sptr | Apache-2.0 OR MIT |
| stable_deref_trait | Apache-2.0 OR MIT |
| syn | Apache-2.0 OR MIT |
| target-lexicon | Apache-2.0 WITH LLVM-exception |
| thiserror | Apache-2.0 OR MIT |
| thiserror-impl | Apache-2.0 OR MIT |
| toml | Apache-2.0 OR MIT |
| tracing-attributes | MIT |
| tracing-core | MIT |
| tracing | MIT |
| typenum | Apache-2.0 OR MIT |
| unicode-ident | (MIT OR Apache-2.0) AND Unicode-DFS-2016 |
| unicode-segmentation | Apache-2.0 OR MIT |
| unicode-width | Apache-2.0 OR MIT |
| unicode-xid | Apache-2.0 OR MIT |
| uuid | Apache-2.0 OR MIT |
| wasm-encoder | Apache-2.0 WITH LLVM-exception |
| wasmparser | Apache-2.0 WITH LLVM-exception |
| wasmprinter | Apache-2.0 WITH LLVM-exception |
| wasmtime | Apache-2.0 WITH LLVM-exception |
| wasmtime-asm-macros | Apache-2.0 WITH LLVM-exception |
| wasmtime-cache | Apache-2.0 WITH LLVM-exception |
| wasmtime-c-api-impl | Apache-2.0 WITH LLVM-exception |
| wasmtime-c-api-macros | Apache-2.0 WITH LLVM-exception |
| wasmtime-component-macro | Apache-2.0 WITH LLVM-exception |
| wasmtime-component-util | Apache-2.0 WITH LLVM-exception |
| wasmtime-cranelift | Apache-2.0 WITH LLVM-exception |
| wasmtime-cranelift-shared | Apache-2.0 WITH LLVM-exception |
| wasmtime-environ | Apache-2.0 WITH LLVM-exception |
| wasmtime-fiber | Apache-2.0 WITH LLVM-exception |
| wasmtime-jit | Apache-2.0 WITH LLVM-exception |
| wasmtime-jit-debug | Apache-2.0 WITH LLVM-exception |
| wasmtime-jit-icache-coherence | Apache-2.0 WITH LLVM-exception |
| wasmtime-runtime | Apache-2.0 WITH LLVM-exception |
| wasmtime-types | Apache-2.0 WITH LLVM-exception |
| wasmtime-versioned-export-macros | Apache-2.0 WITH LLVM-exception |
| wasmtime-winch | Apache-2.0 WITH LLVM-exception |
| wasmtime-wit-bindgen | Apache-2.0 WITH LLVM-exception |
| wasmtime-wmemcheck | Apache-2.0 WITH LLVM-exception |
| wast | Apache-2.0 WITH LLVM-exception |
| wat | Apache-2.0 WITH LLVM-exception |
| winch-codegen | Apache-2.0 WITH LLVM-exception |
| wit-parser | Apache-2.0 WITH LLVM-exception |
| zstd | MIT |
| zstd-safe | Apache-2.0 OR MIT |
| zstd-sys | Apache-2.0 OR MIT |

# Go deps _used_ in production in RPK (exclude all test dependencies)

| software                                              | license                   |
| :----------                                           | :------------:            |
| cloud.google.com/go                                   | Apache License 2.0        |
| github.com/AlecAivazis/survey/v2                      | MIT License               |
| github.com/avast/retry-go                             | MIT License               |
| github.com/aws/aws-sdk-go                             | Apache License 2.0        |
| github.com/beevik/ntp                                 | BSD 2-Clause License      |
| github.com/cespare/xxhash                             | MIT License               |
| github.com/coreos/go-systemd/v22                      | Apache License 2.0        |
| github.com/docker/docker                              | Apache License 2.0        |
| github.com/docker/go-connections                      | Apache License 2.0        |
| github.com/docker/go-units                            | Apache License 2.0        |
| github.com/fatih/color                                | MIT License               |
| github.com/hashicorp/go-multierror                    | MPL-2.0                   |
| github.com/lestrrat-go/jwx                            | MIT License               |
| github.com/lorenzosaino/go-sysctl                     | BSD 3-Clause License      |
| github.com/mattn/go-runewidth                         | MIT License               |
| github.com/mitchellh/colorstring                      | MIT License               |
| github.com/moby/term                                  | Apache License 2.0        |
| github.com/opencontainers/image-spec/specs-go         | Apache License 2.0        |
| github.com/pkg/browser                                | BSD 2-Clause License      |
| github.com/pkg/errors                                 | BSD 2-Clause License      |
| github.com/prometheus/client_model/go                 | Apache License 2.0        |
| github.com/prometheus/common                          | Apache License 2.0        |
| github.com/rivo/uniseg                                | MIT License               |
| github.com/safchain/ethtool                           | Apache License 2.0        |
| github.com/schollz/progressbar/v3                     | MIT License               |
| github.com/sethgrid/pester                            | MIT License               |
| github.com/spf13/afero                                | Apache License 2.0        |
| github.com/spf13/cobra                                | Apache License 2.0        |
| github.com/spf13/pflag                                | BSD 3-Clause License      |
| github.com/tklauser/go-sysconf                        | BSD 3-Clause License      |
| github.com/twmb/franz-go/pkg                          | BSD 3-Clause License      |
| github.com/twmb/franz-go/pkg/kadm                     | BSD 3-Clause License      |
| github.com/twmb/franz-go/pkg/kmsg                     | BSD 3-Clause License      |
| github.com/twmb/tlscfg                                | BSD 3-Clause License      |
| github.com/twmb/types                                 | BSD 3-Clause License      |
| go.uber.org/zap                                       | MIT License               |
| golang.org/x/sync/errgroup                            | BSD 3-Clause License      |
| golang.org/x/sys/unix                                 | BSD 3-Clause License      |
| golang.org/x/term                                     | BSD 3-Clause License      |
| gopkg.in/yaml.v3                                      | MIT License               |
| k8s.io/api                                            | Apache License 2.0        |
| k8s.io/apimachinery/pkg                               | Apache License 2.0        |
| k8s.io/client-go                                      | Apache License 2.0        |

# Go deps _used_ in production in K8S (exclude all test dependencies)

| software                                  | license                   |
| :----------                               | :------------:            |
| github.com/banzaicloud/k8s-objectmatcher  | Apache License 2          |
| github.com/go-logr/logr                   | Apache License 2          |
| github.com/hashicorp/go-multierror        | Mozilla Public License 2.0|
| github.com/jetstack/cert-manager          | Apache License 2          |
| github.com/prometheus/client_golang       | Apache License 2          |
| github.com/spf13/afero                    | Apache License 2          |
| gopkg.in/yaml.v3                          | Apache License 2          |
| k8s.io/api                                | Apache License 2          |
| k8s.io/apimachinery                       | Apache License 2          |
| k8s.io/client-go                          | Apache License 2          |
| k8s.io/utils                              | Apache License 2          |
| sigs.k8s.io/controller-runtime            | Apache License 2          |
