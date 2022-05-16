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
#pragma once

#include "cluster/types.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "storage/types.h"
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

/**
 * A snapshot of node-local state: i.e. things that don't depend on consensus.
 */
struct local_state {
    application_version redpanda_version;
    cluster_version logical_version{invalid_version};
    std::chrono::milliseconds uptime;
    // Eventually support multiple volumes.
    std::vector<storage::disk> disks;

    storage::disk_space_alert storage_space_alert;

    friend std::ostream& operator<<(std::ostream&, const local_state&);
};

} // namespace cluster::node

namespace reflection {
template<>
struct adl<storage::disk> {
    void to(iobuf&, storage::disk&&);
    storage::disk from(iobuf_parser&);
};
} // namespace reflection