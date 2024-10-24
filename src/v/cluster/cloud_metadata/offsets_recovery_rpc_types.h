/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/errc.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/envelope.h"
#include "ssx/sformat.h"
#include "utils/retry_chain_node.h"

namespace cluster::cloud_metadata {

// Request to restore the given groups. It is expected that each group in this
// request maps to the same offset topic partition.
struct offsets_recovery_request
  : public serde::envelope<
      offsets_recovery_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp offsets_ntp;
    cloud_storage_clients::bucket_name bucket;
    std::vector<ss::sstring> offsets_snapshot_paths;

    auto serde_fields() {
        return std::tie(offsets_ntp, bucket, offsets_snapshot_paths);
    }

    friend bool
    operator==(const offsets_recovery_request&, const offsets_recovery_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offsets_recovery_request& r) {
        o << ssx::sformat(
          "{{ntp: {}, bucket: {}, offsets_snapshot_paths: {}}}",
          r.offsets_ntp,
          r.bucket,
          r.offsets_snapshot_paths);
        return o;
    }
};

// Result of a restore request.
struct offsets_recovery_reply
  : public serde::envelope<
      offsets_recovery_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    cluster::errc ec{};

    auto serde_fields() { return std::tie(ec); }

    friend bool
    operator==(const offsets_recovery_reply&, const offsets_recovery_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offsets_recovery_reply& r) {
        o << ssx::sformat("{{ec: {}}}", r.ec);
        return o;
    }
};

class offsets_recovery_requestor {
public:
    virtual ss::future<error_outcome> recover(
      retry_chain_node& parent_retry,
      const cloud_storage_clients::bucket_name& bucket,
      std::vector<std::vector<cloud_storage::remote_segment_path>>)
      = 0;
    virtual ~offsets_recovery_requestor() = default;
};

} // namespace cluster::cloud_metadata
