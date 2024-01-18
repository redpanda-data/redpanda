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

#include "cluster/cloud_metadata/types.h"
#include "cluster/errc.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "serde/envelope.h"

namespace cluster::cloud_metadata {

struct offsets_upload_request
  : public serde::envelope<
      offsets_upload_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    // Cluster UUID with which to associate the uploaded metadata.
    model::cluster_uuid cluster_uuid;

    // NTP of the offsets topic partition.
    model::ntp offsets_ntp;

    // Cluster metadata ID with which to associate the uploaded metadata.
    cluster_metadata_id meta_id;

    auto serde_fields() { return std::tie(cluster_uuid, offsets_ntp, meta_id); }

    friend bool
    operator==(const offsets_upload_request&, const offsets_upload_request&)
      = default;
};

struct offsets_upload_reply
  : public serde::envelope<
      offsets_upload_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    cluster::errc ec{};

    // Remote paths that were successfully uploaded.
    std::vector<ss::sstring> uploaded_paths{};

    auto serde_fields() { return std::tie(ec, uploaded_paths); }

    friend bool
    operator==(const offsets_upload_reply&, const offsets_upload_reply&)
      = default;
};

class offsets_upload_requestor {
public:
    virtual ss::future<offsets_upload_reply>
      request_upload(offsets_upload_request, model::timeout_clock::duration)
      = 0;
};

} // namespace cluster::cloud_metadata
