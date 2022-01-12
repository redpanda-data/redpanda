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
#pragma once

#include "model/metadata.h"
#include "types.h"
#include "utils/human.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <fmt/ostream.h>

namespace cluster::node {

//
//  Node-local state. Includes things like:
//  - Current free resources (disk).
//  - Software versions (OS, redpanda, etc.).
//

using application_version = named_type<ss::sstring, struct version_number_tag>;

struct disk {
    static constexpr int8_t current_version = 0;

    ss::sstring path;
    uint64_t free;
    uint64_t total;

    friend std::ostream& operator<<(std::ostream&, const disk&);
    friend bool operator==(const disk&, const disk&) = default;
};

/**
 * A snapshot of node-local state: i.e. things that don't depend on consensus.
 */
struct local_state {
    application_version redpanda_version;
    std::chrono::milliseconds uptime;
    // Eventually support multiple volumes.
    std::vector<disk> disks;

    friend std::ostream& operator<<(std::ostream&, const local_state&);
};

std::ostream& operator<<(std::ostream& o, const disk& d);
std::ostream& operator<<(std::ostream& o, const local_state& s);

} // namespace cluster::node