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
struct local_state
  : serde::envelope<local_state, serde::version<1>, serde::compat_version<0>> {
    application_version redpanda_version;
    cluster_version logical_version{invalid_version};
    std::chrono::milliseconds uptime;

    // Depending on how the operating system is configured, these may point
    // to the same state if the cache & data dirs share a drive.
    storage::disk data_disk;
    std::optional<storage::disk> cache_disk;

    // Get a reference to the cache disk, which will be the same
    // object as the data disk if shared_disk() is true
    storage::disk& get_cache_disk() {
        if (cache_disk.has_value()) {
            return cache_disk.value();
        } else {
            return data_disk;
        }
    }

    bool shared_disk() const { return !cache_disk.has_value(); }

    /// Report a generalized node-wide disk alert state: this is the worst of
    /// all drive's alert state.
    storage::disk_space_alert get_disk_alert() const;

    void serde_read(iobuf_parser&, const serde::header&);
    void serde_write(iobuf& out) const;

    /// For tests + serialization, where we would like an array of disks,
    /// which is of length 1 or 2, depending on shared_disk() (data disk comes
    /// first).
    std::vector<storage::disk> disks() const;
    void set_disk(storage::disk);
    void set_disks(std::vector<storage::disk>);

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
