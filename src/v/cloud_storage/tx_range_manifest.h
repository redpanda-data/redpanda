/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/base_manifest.h"
#include "json/document.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"

#include <rapidjson/document.h>

#include <optional>

namespace cloud_storage {

/// Transactional metadata path in S3
remote_manifest_path generate_remote_tx_path(const remote_segment_path& path);

class tx_range_manifest final : public base_manifest {
public:
    /// Create manifest for specific ntp
    explicit tx_range_manifest(
      remote_segment_path spath, fragmented_vector<model::tx_range> range);

    /// Create empty manifest that supposed to be updated later
    explicit tx_range_manifest(remote_segment_path spath);

    friend bool
    operator==(const tx_range_manifest& lhs, const tx_range_manifest& rhs) {
        return lhs._path == rhs._path && lhs._ranges == rhs._ranges;
    }

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char> is) override;
    void update(const rapidjson::Document& is);

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    ss::future<serialized_json_stream> serialize() const override;

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const override;

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize(std::ostream& out) const;

    manifest_type get_manifest_type() const override {
        return manifest_type::tx_range;
    };

    fragmented_vector<model::tx_range>&& get_tx_range() && {
        return std::move(_ranges);
    }

private:
    remote_segment_path _path;
    fragmented_vector<model::tx_range> _ranges;
};
} // namespace cloud_storage
