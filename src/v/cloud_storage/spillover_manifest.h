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
#include "model/metadata.h"

#include <stdexcept>

namespace cloud_storage {

inline remote_manifest_path generate_spillover_manifest_path(
  const model::ntp& ntp,
  model::initial_revision_id rev,
  model::offset base,
  model::offset last) {
    auto path = generate_partition_manifest_path(ntp, rev);
    return remote_manifest_path(fmt::format("{}.{}.{}", path, base(), last()));
}

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

    remote_manifest_path get_manifest_path() const override {
        auto so = get_start_offset();
        if (!so.has_value()) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Can't generate path for the empty spillover manifest"));
        }
        return generate_spillover_manifest_path(
          get_ntp(), get_revision_id(), so.value(), get_last_offset());
    }
};

} // namespace cloud_storage