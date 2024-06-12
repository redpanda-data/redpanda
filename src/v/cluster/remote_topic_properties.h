// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "reflection/adl.h"
#include "serde/envelope.h"

namespace cluster {

struct remote_topic_properties
  : serde::envelope<
      remote_topic_properties,
      serde::version<0>,
      serde::compat_version<0>> {
    remote_topic_properties() = default;
    remote_topic_properties(
      model::initial_revision_id remote_revision,
      int32_t remote_partition_count)
      : remote_revision(remote_revision)
      , remote_partition_count(remote_partition_count) {}

    model::initial_revision_id remote_revision;
    int32_t remote_partition_count;

    auto serde_fields() {
        return std::tie(remote_revision, remote_partition_count);
    }

    friend bool
    operator==(const remote_topic_properties&, const remote_topic_properties&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const remote_topic_properties&);
};

} // namespace cluster
// namespace cluster

namespace reflection {

template<>
struct adl<cluster::remote_topic_properties> {
    void to(iobuf&, cluster::remote_topic_properties&&);
    cluster::remote_topic_properties from(iobuf_parser&);
};

} // namespace reflection
