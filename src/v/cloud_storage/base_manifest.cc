/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/base_manifest.h"

namespace cloud_storage {

base_manifest::~base_manifest() = default;

std::ostream& operator<<(std::ostream& s, manifest_type t) {
    switch (t) {
    case manifest_type::topic:
        s << "topic";
        break;
    case manifest_type::partition:
        s << "partition";
        break;
    case manifest_type::tx_range:
        s << "tx-range";
        break;
    case manifest_type::cluster_metadata:
        s << "cluster-metadata";
        break;
    case manifest_type::spillover:
        s << "spillover";
        break;
    case manifest_type::topic_mount:
        s << "topic_mount";
        break;
    }
    return s;
}

std::ostream& operator<<(std::ostream& os, manifest_format f) {
    switch (f) {
    case manifest_format::json:
        return os << "json";
    case manifest_format::serde:
        return os << "serde";
    }
}

ss::future<serialized_data_stream> base_manifest::serialize() const {
    auto buf = co_await serialize_buf();
    auto size = buf.size_bytes();
    co_return serialized_data_stream{
      .stream = make_iobuf_input_stream(std::move(buf)),
      .size_bytes = size,
    };
}

} // namespace cloud_storage
