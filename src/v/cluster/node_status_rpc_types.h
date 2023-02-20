/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/metadata.h"
#include "serde/serde.h"

namespace cluster {

struct node_status_metadata
  : serde::envelope<
      node_status_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id node_id;

    friend std::ostream&
    operator<<(std::ostream& o, const node_status_metadata& nsm) {
        fmt::print(o, "{{node_id:{}}}", nsm.node_id);
        return o;
    }
};

struct node_status_request
  : serde::envelope<
      node_status_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    node_status_metadata sender_metadata;

    friend std::ostream&
    operator<<(std::ostream& o, const node_status_request& r) {
        fmt::print(o, "{{sender_metadata: {}}}", r.sender_metadata);
        return o;
    }
};

struct node_status_reply
  : serde::
      envelope<node_status_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    node_status_metadata replier_metadata;

    friend std::ostream&
    operator<<(std::ostream& o, const node_status_reply& r) {
        fmt::print(o, "{{replier_metadata: {}}}", r.replier_metadata);
        return o;
    }
};

} // namespace cluster
