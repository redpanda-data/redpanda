/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "types.h"

#include "utils/human.h"
#include "utils/to_string.h"

#include <fmt/chrono.h>
#include <fmt/ostream.h>

#include <chrono>

namespace cluster::node {

std::ostream& operator<<(std::ostream& o, const disk& d) {
    fmt::print(
      o,
      "{{path: {}, free: {}, total: {}}}",
      d.path,
      human::bytes(d.free),
      human::bytes(d.total));
    return o;
}

std::ostream& operator<<(std::ostream& o, const disk_space_alert d) {
    switch (d) {
    case disk_space_alert::ok:
        o << "ok";
        break;
    case disk_space_alert::low_space:
        o << "low_space";
        break;
    case disk_space_alert::degraded:
        o << "degraded";
        break;
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const local_state& s) {
    fmt::print(
      o,
      "{{redpanda_version: {}, uptime: {}, disks: {}}}",
      s.redpanda_version,
      s.uptime,
      s.disks);
    return o;
}

} // namespace cluster::node

namespace reflection {
template<typename T>
void read_and_assert_version(std::string_view type, iobuf_parser& parser) {
    auto version = adl<int8_t>{}.from(parser);
    vassert(
      version <= T::current_version,
      "unsupported version of {}, max_supported version: {}, read version: {}",
      type,
      version,
      T::current_version);
}

void adl<cluster::node::disk>::to(iobuf& out, cluster::node::disk&& s) {
    serialize(out, s.current_version, s.path, s.free, s.total);
}

cluster::node::disk adl<cluster::node::disk>::from(iobuf_parser& p) {
    read_and_assert_version<cluster::node::disk>("cluster::node::disk", p);

    auto path = adl<ss::sstring>{}.from(p);
    auto free = adl<uint64_t>{}.from(p);
    auto total = adl<uint64_t>{}.from(p);

    return cluster::node::disk{
      .path = path,
      .free = free,
      .total = total,
    };
}
} // namespace reflection