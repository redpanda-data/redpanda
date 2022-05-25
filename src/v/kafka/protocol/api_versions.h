/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/protocol/schemata/api_versions_request.h"
#include "kafka/protocol/schemata/api_versions_response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct api_versions_request final {
    using api_type = api_versions_api;

    api_versions_request_data data;

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const api_versions_request& r) {
        return os << r.data;
    }
};

struct api_versions_response final {
    using api_type = api_versions_api;

    api_versions_response_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream&
    operator<<(std::ostream& os, const api_versions_response& r) {
        return os << r.data;
    }

private:
    friend bool operator==(
      const kafka::api_versions_response& a,
      const kafka::api_versions_response& b) {
        return a.data.error_code == b.data.error_code
               && a.data.throttle_time_ms == b.data.throttle_time_ms
               && a.data.finalized_features_epoch
                    == b.data.finalized_features_epoch
               && a.data.api_keys == b.data.api_keys
               && a.data.supported_features == b.data.supported_features
               && a.data.finalized_features == b.data.finalized_features;
    }
};

inline bool operator==(
  const api_versions_response_key& a, const api_versions_response_key& b) {
    return a.api_key == b.api_key && a.min_version == b.min_version
           && a.max_version == b.max_version;
}

inline bool operator==(
  const kafka::supported_feature_key& a,
  const kafka::supported_feature_key& b) {
    return a.name == b.name && a.min_version == b.min_version
           && a.max_version == b.max_version;
}

inline bool operator==(
  const kafka::finalized_feature_key& a,
  const kafka::finalized_feature_key& b) {
    return a.name == b.name && a.max_version_level == b.max_version_level
           && a.min_version_level == b.min_version_level;
}

} // namespace kafka
