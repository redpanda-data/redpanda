// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cloud_storage/partition_path_utils.h"

#include "cloud_storage/remote_label.h"
#include "hashing/xx.h"

namespace cloud_storage {

ss::sstring labeled_partition_manifest_prefix(
  const remote_label& label,
  const model::ntp& ntp,
  model::initial_revision_id rev) {
    return fmt::format(
      "{}/meta/{}_{}", label.cluster_uuid(), ntp.path(), rev());
}

ss::sstring labeled_partition_manifest_path(
  const remote_label& label,
  const model::ntp& ntp,
  model::initial_revision_id rev) {
    return fmt::format(
      "{}/manifest.bin", labeled_partition_manifest_prefix(label, ntp, rev));
}

ss::sstring prefixed_partition_manifest_prefix(
  const model::ntp& ntp, model::initial_revision_id rev) {
    constexpr uint32_t bitmask = 0xF0000000;
    const auto ntp_rev = ssx::sformat("{}_{}", ntp.path(), rev());
    uint32_t hash = bitmask & xxhash_32(ntp_rev.data(), ntp_rev.size());
    return fmt::format("{:08x}/meta/{}", hash, ntp_rev);
}

ss::sstring prefixed_partition_manifest_bin_path(
  const model::ntp& ntp, model::initial_revision_id rev) {
    return fmt::format(
      "{}/manifest.bin", prefixed_partition_manifest_prefix(ntp, rev));
}

ss::sstring prefixed_partition_manifest_json_path(
  const model::ntp& ntp, model::initial_revision_id rev) {
    return fmt::format(
      "{}/manifest.json", prefixed_partition_manifest_prefix(ntp, rev));
}

} // namespace cloud_storage
