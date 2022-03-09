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

void adl<storage::disk>::to(iobuf& out, storage::disk&& s) {
    serialize(out, s.current_version, s.path, s.free, s.total);
}

storage::disk adl<storage::disk>::from(iobuf_parser& p) {
    read_and_assert_version<storage::disk>("storage::disk", p);

    auto path = adl<ss::sstring>{}.from(p);
    auto free = adl<uint64_t>{}.from(p);
    auto total = adl<uint64_t>{}.from(p);

    return storage::disk{
      .path = path,
      .free = free,
      .total = total,
    };
}
} // namespace reflection