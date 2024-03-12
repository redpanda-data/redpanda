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
#include "cloud_storage/types.h"
#include "cluster/types.h"
#include "features/feature_table.h"

#include <optional>

namespace cloud_storage {

struct topic_manifest_handler;
class topic_manifest final : public base_manifest {
public:
    using version_t = named_type<int32_t, struct topic_manifest_version_tag>;
    constexpr static auto first_version = version_t{1};
    // this version introduces the serialization of
    // cluster::topic_configuration, with all its field
    constexpr static auto cluster_topic_configuration_version = version_t{2};

    constexpr static auto current_version = cluster_topic_configuration_version;

    /// Create manifest for specific ntp
    explicit topic_manifest(
      const cluster::topic_configuration& cfg,
      model::initial_revision_id rev,
      const features::feature_table&);

    /// Create empty manifest that supposed to be updated later
    topic_manifest();

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char> is) override;

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    ss::future<serialized_data_stream> serialize() const override;

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path() const override;

    static remote_manifest_path
    get_topic_manifest_path(model::ns ns, model::topic topic);

    /// Serialize manifest object
    ///
    /// \param out output stream that should be used to output the json
    void serialize(std::ostream& out) const;

    manifest_type get_manifest_type() const override {
        return manifest_type::topic;
    };

    model::initial_revision_id get_revision() const noexcept { return _rev; }

    /// Change topic-manifest revision
    void set_revision(model::initial_revision_id id) noexcept { _rev = id; }

    std::optional<cluster::topic_configuration> const&
    get_topic_config() const noexcept {
        return _topic_config;
    }

    /// return the version of the decoded manifest. useful to decide if to fill
    /// a field that was not encoded in a previous version
    version_t get_manifest_version() const noexcept {
        return _manifest_version;
    }

    bool operator==(const topic_manifest& other) const {
        return std::tie(_topic_config, _rev)
               == std::tie(other._topic_config, other._rev);
    };

private:
    std::optional<cluster::topic_configuration> _topic_config;
    model::initial_revision_id _rev;
    version_t _manifest_version{first_version};
};
} // namespace cloud_storage
