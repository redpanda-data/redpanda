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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace lsm::internal {

// The level in the LSM tree.
using level = named_type<uint8_t, struct level_tag>;

consteval level operator""_level(unsigned long long val) {
    if (val > level::max()()) {
        // This is consteval, so this is a compile time error
        throw std::exception();
    }
    return level{static_cast<uint8_t>(val)};
}

// The numeric ID of an sst file
using file_id = named_type<uint64_t, struct file_id_tag>;

consteval file_id operator""_file_id(unsigned long long val) {
    return file_id{static_cast<uint64_t>(val)};
}

// Database epoch are assigned to a file such that on shared storage mediums
// databases that share persistence locations that write with different epochs
// will not collide with each other.
//
// For example, if replicating a WAL using raft, then writing the WAL to object
// storage, each raft term could become it's own database_epoch, which would
// mean that old leaders cannot clobber a new leader's files while shutting
// down.
using database_epoch = named_type<uint64_t, struct database_epoch_tag>;
consteval database_epoch operator""_db_epoch(unsigned long long val) {
    return database_epoch{val};
}

// File handle is a combination of a file's ID and the database epoch at which
// the file was created at.
struct file_handle {
    file_id id;
    database_epoch epoch;

    bool operator==(const file_handle&) const = default;
    auto operator<=>(const file_handle&) const = default;
    fmt::iterator format_to(fmt::iterator) const;
    template<typename H>
    friend H AbslHashValue(H h, const file_handle& c) {
        return H::combine(std::move(h), c.id, c.epoch);
    }
};

// Compute the name of an sst file with the given ID.
ss::sstring sst_file_name(file_handle) noexcept;

// Parse an sst filename, returning nullopt if the filename pattern is unknown.
std::optional<file_handle>
parse_sst_file_name(std::string_view filename) noexcept;

} // namespace lsm::internal
