module github.com/redpanda-data/redpanda/src/go/rpk

go 1.20

require (
	cloud.google.com/go/compute/metadata v0.2.3
	github.com/AlecAivazis/survey/v2 v2.3.7
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/aws/aws-sdk-go v1.44.300
	github.com/beevik/ntp v1.2.0
	github.com/coreos/go-systemd/v22 v22.5.0
	github.com/docker/docker v24.0.4+incompatible
	github.com/docker/go-connections v0.4.0
	github.com/docker/go-units v0.5.0
	github.com/fatih/color v1.15.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/lestrrat-go/jwx v1.2.26
	github.com/lorenzosaino/go-sysctl v0.3.1
	github.com/moby/term v0.5.0
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.0-rc4
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_model v0.4.0
	github.com/prometheus/common v0.44.0
	github.com/safchain/ethtool v0.3.0
	github.com/schollz/progressbar/v3 v3.13.1
	github.com/sethgrid/pester v1.2.0
	github.com/spf13/afero v1.9.5
	github.com/spf13/cobra v1.7.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.4
	github.com/tklauser/go-sysconf v0.3.11
	github.com/twmb/franz-go v1.14.2
	github.com/twmb/franz-go/pkg/kadm v1.9.0
	github.com/twmb/franz-go/pkg/kmsg v1.6.1
	github.com/twmb/franz-go/pkg/sr v0.0.0-20230804141320-83cb9fe23c6c
	github.com/twmb/franz-go/plugin/kzap v1.1.2
	github.com/twmb/tlscfg v1.2.1
	github.com/twmb/types v1.1.6
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20230713183714-613f0c0eb8a1
	golang.org/x/sync v0.3.0
	golang.org/x/sys v0.10.0
	golang.org/x/term v0.10.0
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/api v0.27.1
	k8s.io/apimachinery v0.27.1
	k8s.io/client-go v0.27.1
)

require (
	cloud.google.com/go/compute v1.21.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/BurntSushi/toml v1.3.2 // indirect
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/emicklei/go-restful/v3 v3.10.2 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.4 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/gnostic v0.6.9 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.1 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/morikuni/aec v0.0.0-20170113033406-39771216ff4c // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/tklauser/numcpus v0.6.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.11.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20230713183714-613f0c0eb8a1 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.12.0 // indirect
	golang.org/x/net v0.12.0 // indirect
	golang.org/x/oauth2 v0.10.0 // indirect
	golang.org/x/text v0.11.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.11.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gotest.tools/v3 v3.0.3 // indirect
	honnef.co/go/tools v0.4.3 // indirect
	k8s.io/klog/v2 v2.100.1 // indirect
	k8s.io/kube-openapi v0.0.0-20230501164219-8b0f38b5fd1f // indirect
	k8s.io/utils v0.0.0-20230505201702-9f6742963106 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
