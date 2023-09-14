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
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/envelope.h"
#include "utils/fragmented_vector.h"

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

    ~offsets_upload_request() = default;
    offsets_upload_request() = default;
    offsets_upload_request(const offsets_upload_request& r) = default;
    offsets_upload_request(offsets_upload_request&&) = default;
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

    offsets_upload_reply(cluster::errc e, std::vector<ss::sstring> p)
      : ec(e)
      , uploaded_paths(std::move(p)) {}
    offsets_upload_reply(const offsets_upload_reply& r)
      : ec(r.ec)
      , uploaded_paths(r.uploaded_paths) {}
    offsets_upload_reply(offsets_upload_reply&& r) = default;
    offsets_upload_reply& operator=(const offsets_upload_reply& r) {
        if (this == &r) {
            return *this;
        }
        ec = r.ec;
        uploaded_paths = r.uploaded_paths;
        return *this;
    }
    offsets_upload_reply() = default;

    auto serde_fields() { return std::tie(ec, uploaded_paths); }

    friend bool
    operator==(const offsets_upload_reply&, const offsets_upload_reply&)
      = default;
};

} // namespace cluster::cloud_metadata
