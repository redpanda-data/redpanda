/*
 * Copyright 2021 Redpanda Data, Inc.
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
      "{{redpanda_version: {}, uptime: {}, data_disk: {}, cache_disk: {}}}",
      s.redpanda_version,
      s.uptime,
      s.data_disk,
      s.cache_disk);
    return o;
}

void local_state::serde_read(iobuf_parser& in, const serde::header& h) {
    redpanda_version = serde::read_nested<application_version>(in, 0);
    logical_version = serde::read_nested<cluster_version>(in, 0);
    uptime = serde::read_nested<std::chrono::milliseconds>(in, 0);
    auto disks = serde::read_nested<std::vector<storage::disk>>(in, 0);

    if (disks.empty()) {
        throw serde::serde_exception("Empty disk vector in local_state");
    }

    // Redpanda <= 23.1 did not explicitly track data+cache drives, just sent
    // a vector.  For compatibility, we retain the vector, and adopt the
    // convention that data disk always comes first, cache disk second.
    set_disks(disks);

    // Version 0 has a node-global space alert, instead of per drive.
    // Later versions encode this field but it may be ignored.
    auto storage_space_alert = serde::read_nested<storage::disk_space_alert>(
      in, 0);
    if (h._version == 0) {
        for (auto& d : disks) {
            d.alert = storage_space_alert;
        }
    }
}

void local_state::serde_write(iobuf& out) const {
    serde::write(out, redpanda_version);
    serde::write(out, logical_version);
    serde::write(out, uptime);
    serde::write(out, disks());
    serde::write(out, get_disk_alert());
}

storage::disk_space_alert local_state::get_disk_alert() const {
    if (cache_disk.has_value()) {
        return storage::max_severity(data_disk.alert, cache_disk.value().alert);
    } else {
        return data_disk.alert;
    }
}

std::vector<storage::disk> local_state::disks() const {
    std::vector<storage::disk> disks;
    disks.push_back(data_disk);
    if (!shared_disk()) {
        disks.push_back(cache_disk.value());
    }
    return disks;
}

void local_state::set_disk(storage::disk d) {
    data_disk = d;
    cache_disk = std::nullopt;
}

void local_state::set_disks(std::vector<storage::disk> v) {
    if (v.size() == 0) {
        // This is invalid input, but we can cope gracefully by ignoring it
    } else {
        data_disk = v[0];
        if (v.size() > 1) {
            cache_disk = v[1];
        } else {
            cache_disk = std::nullopt;
        }
    }
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