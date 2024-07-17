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

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/types.h"
#include "model/metadata.h"

#include <stdexcept>

namespace cloud_storage {

/// The section of the partition manifest
///
/// The only purpose of this class is to provide different implementation of the
/// 'get_manifest_path' method which includes base offset and last offset of the
/// manifest. These fields are used to collect all manifests stored in the
/// bucket. The name changes when the content of the manifest changes.
class spillover_manifest final : public partition_manifest {
public:
    spillover_manifest(const model::ntp& ntp, model::initial_revision_id rev)
      : partition_manifest(ntp, rev) {}

    remote_manifest_path get_manifest_path(
      const remote_path_provider& path_provider) const override {
        return remote_manifest_path{
          path_provider.partition_manifest_path(*this)};
    }

    static ss::sstring filename(const spillover_manifest_path_components& c) {
        return fmt::format(
          "{}.{}.{}.{}.{}.{}.{}",
          partition_manifest::filename(),
          c.base(),
          c.last(),
          c.base_kafka(),
          c.next_kafka(),
          c.base_ts.value(),
          c.last_ts.value());
    }

    ss::sstring get_manifest_filename() const override {
        const auto ls = last_segment();
        vassert(ls.has_value(), "Spillover manifest can't be empty");
        const auto fs = *begin();
        spillover_manifest_path_components c{
          .base = fs.base_offset,
          .last = ls->committed_offset,
          .base_kafka = fs.base_kafka_offset(),
          .next_kafka = ls->next_kafka_offset(),
          .base_ts = fs.base_timestamp,
          .last_ts = ls->max_timestamp,
        };
        return filename(c);
    }

    manifest_type get_manifest_type() const override {
        return manifest_type::spillover;
    }
};

} // namespace cloud_storage
