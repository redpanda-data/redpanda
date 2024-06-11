// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/remote_topic_properties.h"

#include "reflection/adl.h"

namespace cluster {

std::ostream& operator<<(std::ostream& o, const remote_topic_properties& rtps) {
    fmt::print(
      o,
      "{{remote_revision: {} remote_partition_count: {}}}",
      rtps.remote_revision,
      rtps.remote_partition_count);
    return o;
}

} // namespace cluster

namespace reflection {

void adl<cluster::remote_topic_properties>::to(
  iobuf& out, cluster::remote_topic_properties&& p) {
    reflection::serialize(out, p.remote_revision, p.remote_partition_count);
}

cluster::remote_topic_properties
adl<cluster::remote_topic_properties>::from(iobuf_parser& parser) {
    auto remote_revision = reflection::adl<model::initial_revision_id>{}.from(
      parser);
    auto remote_partition_count = reflection::adl<int32_t>{}.from(parser);

    return {remote_revision, remote_partition_count};
}

} // namespace reflection
