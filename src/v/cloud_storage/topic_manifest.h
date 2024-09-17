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
#include "cloud_storage/fwd.h"
#include "cloud_storage/types.h"
#include "cluster/topic_configuration.h"

#include <optional>

namespace cloud_storage {

struct topic_manifest_handler;
class topic_manifest final : public base_manifest {
public:
    using version_t = named_type<int32_t, struct topic_manifest_version_tag>;

    constexpr static auto first_version = version_t{1};
    // this version introduces the serialization of
    // cluster::topic_configuration, with all its field
    constexpr static auto serde_version = version_t{2};

    constexpr static auto current_version = serde_version;

    /// Create manifest for specific topic.
    explicit topic_manifest(
      const cluster::topic_configuration& cfg, model::initial_revision_id rev);

    /// Create empty manifest that supposed to be updated later
    topic_manifest();

    ss::future<> update(ss::input_stream<char> is) override {
        // assume format is serde
        return update(manifest_format::serde, std::move(is));
    }

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(manifest_format, ss::input_stream<char> is) override;

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    ss::future<iobuf> serialize_buf() const override;

    /// Manifest object name in S3
    remote_manifest_path get_manifest_path(const remote_path_provider&) const;

    /// Serialize manifest object in json format. only fields up to
    /// first_version are serialized
    ///
    /// \param out output stream that should be used to output the json
    void serialize_v1_json(std::ostream& out) const;

    manifest_type get_manifest_type() const override {
        return manifest_type::topic;
    };

    model::initial_revision_id get_revision() const noexcept { return _rev; }

    /// Change topic-manifest revision
    void set_revision(model::initial_revision_id id) noexcept { _rev = id; }

    const std::optional<cluster::topic_configuration>&
    get_topic_config() const noexcept {
        return _topic_config;
    }

    bool operator==(const topic_manifest& other) const {
        return std::tie(_topic_config, _rev)
               == std::tie(other._topic_config, other._rev);
    };

    /// Name to address this manifest by. Note that the exact path is not
    /// tracked by the manifest.
    ss::sstring display_name() const;

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void do_update(const topic_manifest_handler& handler);

    std::optional<cluster::topic_configuration> _topic_config;
    model::initial_revision_id _rev;
};
} // namespace cloud_storage
