/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>

#include <absl/container/node_hash_set.h>

namespace cloud_storage::inventory {

enum class lookup_result {
    exists,
    missing,
    possible_collision,
};

class ntp_path_hashes {
public:
    ntp_path_hashes(model::ntp ntp, std::filesystem::path hash_store_path);

    ntp_path_hashes(const ntp_path_hashes&) = delete;
    ntp_path_hashes& operator=(const ntp_path_hashes&) = delete;

    ntp_path_hashes(ntp_path_hashes&&) noexcept;
    ntp_path_hashes& operator=(ntp_path_hashes&&) = delete;

    virtual ~ntp_path_hashes() = default;

    ss::future<bool> load_hashes();

    lookup_result exists(const remote_segment_path& path) const;

    ss::future<> stop();

    bool loaded() const { return _loaded; }

private:
    ss::future<> load_hashes(ss::directory_entry de);
    ss::future<> load_hashes(ss::input_stream<char>& stream);

    model::ntp _ntp;
    std::filesystem::path _ntp_hashes_path;
    absl::node_hash_set<uint64_t> _path_hashes;
    absl::node_hash_set<uint64_t> _possible_collisions;
    bool _loaded{false};
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;
    ss::gate _gate;
};

} // namespace cloud_storage::inventory
