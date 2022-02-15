/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/base_manifest.h"
#include "json/document.h"

namespace cloud_storage {

class topic_manifest final : public base_manifest {
public:
    /// Create manifest for specific ntp
    explicit topic_manifest(
      const cluster::topic_configuration& cfg, model::initial_revision_id rev);

    /// Create empty manifest that supposed to be updated later
    topic_manifest();

    /// Update manifest file from input_stream (remote set)
    ss::future<> update(ss::input_stream<char> is) override;

    /// Serialize manifest object
    ///
    /// \return asynchronous input_stream with the serialized json
    serialized_json_stream serialize() const override;

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

private:
    /// Update manifest content from json document that supposed to be generated
    /// from manifest.json file
    void update(const json::Document& m);

    std::optional<cluster::topic_configuration> _topic_config;
    model::initial_revision_id _rev;
};
} // namespace cloud_storage
