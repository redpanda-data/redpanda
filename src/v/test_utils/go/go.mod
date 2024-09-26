module kafka-request-generator

// TODO: ubuntu:mantic and fedora:38 only have go version v1.21, so we are
// stuck with 1.21. This is until we upgrade the OS used for the open
// source CI build. (hopefully soon, once we can also build redpanda with
// clang-18)
go 1.21

require (
	github.com/kr/pretty v0.3.1
	github.com/parquet-go/parquet-go v0.23.0
	github.com/segmentio/encoding v0.4.0
	github.com/twmb/franz-go/pkg/kmsg v1.8.0
)

require (
	github.com/andybalholm/brotli v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.17.10 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
)
