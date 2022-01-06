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

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "reflection/adl.h"
#include "types.h"
#include "utils/human.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <fmt/ostream.h>

namespace cluster::node {

//   _   _           _            _                 _       _        _
//  | \ | | ___   __| | ___      | | ___   ___ __ _| |  ___| |_ __ _| |_ ___
//  |  \| |/ _ \ / _` |/ _ \_____| |/ _ \ / __/ _` | | / __| __/ _` | __/ _ \.
//  | |\  | (_) | (_| |  __/_____| | (_) | (_| (_| | | \__ \ || (_| | ||  __/_
//  |_| \_|\___/ \__,_|\___|     |_|\___/ \___\__,_|_| |___/\__\__,_|\__\___(_)
//
//  Node-local state. Includes things like:
//  - Current free resources (disk).
//  - Software versions (OS, redpanda, etc.).
//  Could eventually include:
//  - Hardware enumeration, configuration, and statistics.
//

using application_version = named_type<ss::sstring, struct version_number_tag>;

struct disk {
    static constexpr int8_t current_version = 0;

    ss::sstring path;
    uint64_t free;
    uint64_t total;
    // uint32_t fstype;    // AF TODO? an enum for fs type?

    friend std::ostream& operator<<(std::ostream&, const disk&);
    friend bool operator==(const disk&, const disk&) = default;
};

/**
 * Node-local state sample. report is collected built based on node local state
 * at given instance of time
 */
struct local_state {
    static constexpr int8_t current_version = 0;

    application_version redpanda_version;
    std::chrono::milliseconds uptime;
    // Local time when state was collected
    // XXX AF: how to annotate NO SERIALIZE?
    model::timestamp timestamp;
    // Eventually support multiple volumes.
    std::vector<disk> disks;

    friend std::ostream& operator<<(std::ostream&, const local_state&);
};

std::ostream& operator<<(std::ostream& o, const disk& d);
std::ostream& operator<<(std::ostream& o, const local_state& s);

} // namespace cluster::node

namespace reflection {
template<>
struct adl<cluster::node::local_state> {
    void to(iobuf&, cluster::node::local_state&&);
    cluster::node::local_state from(iobuf_parser&);
};

template<>
struct adl<cluster::node::disk> {
    void to(iobuf&, cluster::node::disk&&);
    cluster::node::disk from(iobuf_parser&);
};
} // namespace reflection
