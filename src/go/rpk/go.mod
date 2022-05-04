module github.com/redpanda-data/redpanda/src/go/rpk

go 1.17

require (
	cloud.google.com/go v0.46.3
	github.com/AlecAivazis/survey/v2 v2.3.2
	github.com/avast/retry-go v2.6.0+incompatible
	github.com/aws/aws-sdk-go v1.25.43
	github.com/beevik/ntp v0.3.0
	github.com/cespare/xxhash v1.1.0
	github.com/cockroachdb/crlfmt v0.0.0-20210128092314-b3eff0b87c79
	github.com/coreos/go-systemd/v22 v22.1.0
	github.com/docker/docker v20.10.6+incompatible
	github.com/docker/go-connections v0.4.0
	github.com/docker/go-units v0.4.0
	github.com/fatih/color v1.7.0
	github.com/google/uuid v1.1.1
	github.com/hashicorp/go-multierror v1.1.0
	github.com/icza/dyno v0.0.0-20200205103839-49cb13720835
	github.com/lorenzosaino/go-sysctl v0.1.0
	github.com/mitchellh/mapstructure v1.4.1
	github.com/olekukonko/tablewriter v0.0.1
	github.com/opencontainers/image-spec v1.0.1
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/safchain/ethtool v0.0.0-20190326074333-42ed695e3de8
	github.com/sethgrid/pester v1.1.0
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/afero v1.6.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.0
	github.com/stretchr/testify v1.7.0
	github.com/tklauser/go-sysconf v0.1.0
	github.com/twmb/franz-go v1.5.1
	github.com/twmb/franz-go/pkg/kadm v0.0.0-20220106030238-d62f1bef74f9
	github.com/twmb/franz-go/pkg/kmsg v1.0.0
	github.com/twmb/tlscfg v1.2.0
	github.com/twmb/types v1.1.6
	golang.org/x/crypto v0.0.0-20220427172511-eb4f295cb31f
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20211101204403-39c9dd37992c
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	mvdan.cc/sh/v3 v3.2.1
)

require (
	github.com/Microsoft/go-winio v0.4.12 // indirect
	github.com/cockroachdb/gostdlib v1.13.0 // indirect
	github.com/cockroachdb/ttycolor v0.0.0-20180709150743-a1d5aaeb377d // indirect
	github.com/containerd/containerd v1.4.8 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/godbus/dbus/v5 v5.0.3 // indirect
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/google/renameio v0.1.0 // indirect
	github.com/gorilla/mux v1.7.3 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.0.0-20180206201540-c2b33e8439af // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/klauspost/compress v1.15.2 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/magiconair/properties v1.8.1 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.4 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/moby/term v0.0.0-20201216013528-df9cb8a40635 // indirect
	github.com/morikuni/aec v0.0.0-20170113033406-39771216ff4c // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pelletier/go-toml v1.2.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.14 // indirect
	github.com/pkg/diff v0.0.0-20200914180035-5b29258ca4f7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/cast v1.3.0 // indirect
	github.com/spf13/jwalterweatherman v1.0.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tklauser/numcpus v0.1.0 // indirect
	github.com/twmb/go-rbtree v1.0.0 // indirect
	golang.org/x/mod v0.4.1 // indirect
	golang.org/x/net v0.0.0-20211123203042-d83791d6bcd9 // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.0.0-20210114065538-d78b04bdf963 // indirect
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013 // indirect
	google.golang.org/grpc v1.27.0 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/ini.v1 v1.51.0 // indirect
	gotest.tools/v3 v3.0.3 // indirect
	mvdan.cc/editorconfig v0.1.1-0.20200121172147-e40951bde157 // indirect
)
