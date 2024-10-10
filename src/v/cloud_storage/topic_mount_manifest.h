/*
 * Copyright 2024 Redpanda Data, Inc.
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
#include "cloud_storage/remote_label.h"
#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/core/future.hh>

#include <optional>

namespace cloud_storage {

class topic_mount_manifest final : public base_manifest {
public:
    topic_mount_manifest(
      const remote_label& label,
      const model::topic_namespace& tp_ns,
      model::initial_revision_id rev);

    ss::future<> update(ss::input_stream<char> is) override;

    ss::future<iobuf> serialize_buf() const override;

    const remote_label& get_source_label() const { return _source_label; }

    const model::topic_namespace& get_tp_ns() const { return _tp_ns; }

    model::initial_revision_id get_revision_id() const { return _rev; }

    /// Manifest object name in S3
    remote_manifest_path
    get_manifest_path(const remote_path_provider& path_provider) const;

    manifest_type get_manifest_type() const override;

private:
    void from_iobuf(iobuf in);

    // The label for the cluster that originally created the topic to be
    // mounted/unmounted.
    remote_label _source_label;
    model::topic_namespace _tp_ns;
    model::initial_revision_id _rev;
};
} // namespace cloud_storage
