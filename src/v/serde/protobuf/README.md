# Protobuf Parser

This directory contains a seastar friendly protobuf parser.
It should adhere to the same compatibility guarantees as the offical C++ library
but has a few notable differences:

1. Does not make contiguous allocations of repeated fields, maps or strings/bytes types
2. Is reactor friendly on deeply nested or large protobufs in that it will yield control
3. Is a stackless parser, so it is not bound by the smallish 1MB stacks that seastar uses for threads

## Development

If you are tasked with updating this code, here are a few helpful links:

1. [Encoding spec](https://protobuf.dev/programming-guides/encoding/) (note this elides some important details about how invalid/corrupted data is handled)
2. [Golang protobuf parser](https://github.com/protocolbuffers/protobuf-go/blob/master/proto/decode.go)
3. [Java protobuf parser](https://github.com/protocolbuffers/protobuf/tree/main/java/core/src/main/java/com/google/protobuf)
4. [C++ protobuf parser](https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/wire_format_lite.cc)
5. [Protobuf Zero](https://github.com/mapbox/protozero/blob/master/include/protozero/pbf_reader.hpp)
