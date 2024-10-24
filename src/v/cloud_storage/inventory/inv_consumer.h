
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

#include "cloud_storage/inventory/report_parser.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace cloud_storage::inventory {

/// The inventory consumer reads a CSV inventory report, which may be GZip
/// compressed, and stores the hashes for file paths from the report. Each row
/// in the report is expected to contain a path to a segment in the bucket for
/// which the inventory was generated. A set of NTPs is provided to the consumer
/// for validation, and only paths belonging to these NTPs are processed. For
/// each path, its 64 bit hash is calculated and stored in memory. Once the
/// cumulative size in memory of the stored hashes grows over a prescribed
/// limit, the hashes for the invidividual NTPs are written to disk until the
/// cumulative memory usage drops below the desired limit. On completion of the
/// report consumption, all hashes in memory are flushed to disk.
class inventory_consumer {
    using write_all_t = ss::bool_class<struct write_all_tag>;

public:
    inventory_consumer(const inventory_consumer&) = delete;
    inventory_consumer& operator=(const inventory_consumer&) = delete;
    inventory_consumer(inventory_consumer&&) = delete;
    inventory_consumer& operator=(inventory_consumer&&) = delete;

    inventory_consumer(
      std::filesystem::path hash_store_path,
      absl::node_hash_set<model::ntp> ntps,
      size_t max_hash_size_in_memory);

    /// Consumes the report provided as input stream and populates in memory
    /// path hashes. A flush to disk may be performed if the size of hashes
    /// exceeds the prescribed limit.
    ss::future<>
    consume(ss::input_stream<char> s, is_gzip_compressed is_report_compressed);

    static std::optional<model::ntp> ntp_from_path(std::string_view path);
    static std::vector<ss::sstring> parse_row(ss::sstring row);

    /// Ensures that streams opened by consumer are closed. Called during
    /// exception handling if an exception is thrown during a stream
    /// consumption.
    ss::future<> stop();

    ~inventory_consumer() = default;

private:
    ss::future<> process_paths(fragmented_vector<ss::sstring> paths);

    // Checks if the path belongs to one of the NTPs whose leadership belongs to
    // this node. If so, the path is hashed and added to current NTP flush
    // states, and will be written to disk on the next flush operation.
    void process_path(ss::sstring path);

    // Writes the largest hash vectors to disk. The vectors are written in files
    // named after their NTP. If write_all_entries is true, all hashes are
    // written to disk. If it is not true, we write the vectors until total
    // memory used falls below the max hash size limit.
    ss::future<> flush(write_all_t write_all_entries = write_all_t::no);

    // The root directory for hash files
    std::filesystem::path _hash_store_path;

    // NTPs for which leaders are shards on this node. The data produced by this
    // class is expected to be used by the scrubbers which run on this node.
    // Since scrubbing for an NTP is done by its partition leader, we need data
    // for all possible scrubs done via this node, so we collect the NTPs led by
    // this node up front and only process rows related to these.
    absl::node_hash_set<model::ntp> _ntps;

    struct flush_state {
        uint64_t next_file_name{0};
        fragmented_vector<uint64_t> hashes{};
    };
    // Mapping of hash vectors per NTP, populated as paths from inventory are
    // processed.
    absl::node_hash_map<model::ntp, flush_state> _ntp_flush_states;

    // Max size of path hashes (8 bytes each) held in memory before a flush is
    // performed to free up memory.
    size_t _max_hash_size_in_memory;

    // Total size in bytes of path hashes held in memory
    size_t _total_size{};

    size_t _num_flushes{};

    ss::gate _gate;
};

} // namespace cloud_storage::inventory
