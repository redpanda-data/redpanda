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

#include "cluster/errc.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/envelope.h"

namespace cluster::cloud_metadata {

struct offsets_lookup_request
  : public serde::envelope<
      offsets_lookup_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    cluster::errc error;

    // Node ID to which this request is sent.
    model::node_id node_id;

    // List of NTPs being looked up.
    fragmented_vector<model::ntp> ntps;

    auto serde_fields() { return std::tie(node_id, ntps); }

    friend bool
    operator==(const offsets_lookup_request&, const offsets_lookup_request&)
      = default;
};

struct offsets_lookup_reply
  : public serde::envelope<
      offsets_lookup_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    struct ntp_offset
      : public serde::
          envelope<ntp_offset, serde::version<0>, serde::compat_version<0>> {
        using rpc_adl_exempt = std::true_type;

        model::ntp ntp;
        kafka::offset offset;

        ntp_offset(model::ntp n, kafka::offset o)
          : ntp(std::move(n))
          , offset(o) {}
        ntp_offset() = default;

        auto serde_fields() { return std::tie(ntp, offset); }
        friend bool operator==(const ntp_offset&, const ntp_offset&) = default;
    };

    // Node ID from which the reply was created.
    model::node_id node_id;

    // Kakfa end offsets per NTP.
    fragmented_vector<ntp_offset> ntp_and_offset;

    auto serde_fields() { return std::tie(node_id, ntp_and_offset); }

    friend bool
    operator==(const offsets_lookup_reply&, const offsets_lookup_reply&)
      = default;
};

} // namespace cluster::cloud_metadata
