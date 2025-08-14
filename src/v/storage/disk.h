/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "serde/envelope.h"

#include <seastar/core/sstring.hh>

#include <ostream>

namespace storage {

enum class disk_space_alert { ok = 0, low_space = 1, degraded = 2 };

inline disk_space_alert max_severity(disk_space_alert a, disk_space_alert b) {
    return std::max(a, b);
}

inline std::ostream& operator<<(std::ostream& o, const disk_space_alert d) {
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

struct disk
  : serde::envelope<disk, serde::version<1>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    ss::sstring path;
    uint64_t free{0};
    uint64_t total{0};
    disk_space_alert alert{disk_space_alert::ok};

    auto serde_fields() { return std::tie(path, free, total, alert); }

    // this value is _not_ serialized, but having it in this structure is useful
    // for passing the filesystem id around as the structure is used internally
    // to represent a disk not only for marshalling data to disk/network.
    unsigned long int fsid;

    friend std::ostream& operator<<(std::ostream&, const disk&);
    friend bool operator==(const disk&, const disk&) = default;
};

} // namespace storage
