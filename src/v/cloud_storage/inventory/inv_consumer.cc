
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

#include "cloud_storage/inventory/inv_consumer.h"

#include "cloud_storage/inventory/utils.h"
#include "cloud_storage/logger.h"
#include "hashing/xx.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/vector.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/coroutine/as_future.hh>

#include <re2/re2.h>

#include <exception>
#include <ranges>

namespace ranges = std::ranges;
namespace views = std::views;

namespace {
// hash-string/ns/tp/partition_rev/.* OR
// cluster-uuid/ns/tp/partition_rev/.*
const RE2 path_expr{"^[[:xdigit:]-]+/(.*?)/(.*?)/(\\d+)_\\d+/.*?"};

// Holds hashes for a given NTP in memory before they will be flushed to disk.
// One of these structures is held per NTP in a map keyed by the NTP itself.
struct flush_entry {
    model::ntp ntp;
    // The vector of hashes which will be written to disk in the file_name on
    // next flush.
    fragmented_vector<uint64_t> hashes;
    // The file to which hashes will be written. The file name is incremented on
    // each flush, so each file name contains some hashes, and we may end up
    // with multiple files per NTP. The hash loader will read all the files and
    // collect the hashes together.
    uint64_t file_name;
};

// Will remove nested, embedded quotes and spaces. The CSV report format we
// expect should be resilient to this because we only care about path names,
// which contain neither spaces nor quotes, but this is not a real CSV parser.
bool drop_chars(char c) { return c != '"' && !std::isspace(c); }

ss::sstring trim_string(ranges::subrange<char*> r) {
    auto filtered = r | views::filter(drop_chars);
    return {filtered.begin(), filtered.end()};
}

} // namespace

namespace cloud_storage::inventory {

inventory_consumer::inventory_consumer(
  std::filesystem::path hash_store_path,
  absl::node_hash_set<model::ntp> ntps,
  size_t max_paths_in_memory)
  : _hash_store_path{std::move(hash_store_path)}
  , _ntps{std::move(ntps)}
  , _max_hash_size_in_memory{max_paths_in_memory} {}

ss::future<> inventory_consumer::consume(
  ss::input_stream<char> s, is_gzip_compressed is_report_compressed) {
    auto h = _gate.hold();
    report_parser parser{std::move(s), is_report_compressed};
    co_await parser
      .consume([this](auto&& paths) { return process_paths(std::move(paths)); })
      .finally([&parser] { return parser.stop(); });
    co_await flush(write_all_t::yes);
}

std::vector<ss::sstring> inventory_consumer::parse_row(ss::sstring row) {
    std::vector<ss::sstring> pieces;
    for (auto p : row | views::split(',') | views::transform(trim_string)) {
        pieces.emplace_back(p);
    }
    return pieces;
}

ss::future<>
inventory_consumer::process_paths(fragmented_vector<ss::sstring> paths) {
    for (const auto& path : paths) {
        // The row is expected to be in the form:
        // "bucket","path"
        auto pieces = parse_row(path);
        // If a row is malformed we skip it and do not abort processing.
        // This way the rest of the entries are processed, and if there
        // was a path in this row it will be recorded as missing.
        if (pieces.size() != 2) {
            vlog(cst_log.warn, "unexpected row in inventory report: {}", path);
        }
        process_path(pieces[1]);
    }

    if (_total_size >= _max_hash_size_in_memory) {
        co_await flush();
    }
}

void inventory_consumer::process_path(ss::sstring path) {
    if (auto maybe_ntp = ntp_from_path(path);
        maybe_ntp.has_value() && _ntps.contains(maybe_ntp.value())) {
        const auto& ntp = maybe_ntp.value();
        auto hash = xxhash_64(path.data(), path.size());

        if (!_ntp_flush_states.contains(ntp)) {
            _ntp_flush_states.insert({ntp, flush_state{}});
        }

        _ntp_flush_states[ntp].hashes.push_back(hash);
        _total_size += sizeof(hash);
    }
}

ss::future<>
inventory_consumer::flush(inventory_consumer::write_all_t write_all_entries) {
    auto h = _gate.hold();
    vlog(
      cst_log.trace,
      "Flushing to disk, write_all_entries: {}",
      write_all_entries);

    _num_flushes += 1;
    if (write_all_entries) {
        for (auto& [ntp, flush_state] : _ntp_flush_states) {
            if (!flush_state.hashes.empty()) {
                co_await flush_ntp_hashes(
                  _hash_store_path,
                  ntp,
                  std::move(flush_state.hashes),
                  flush_state.next_file_name);
            }
        }
        co_return;
    }

    // Set up a vector to sort entries. The ntp and next filename are not moved
    // out of the map, only the hashes are moved out.
    fragmented_vector<flush_entry> entries;
    ranges::transform(
      _ntp_flush_states, std::back_inserter(entries), [](auto& e) {
          return flush_entry{
            e.first, std::move(e.second.hashes), e.second.next_file_name};
      });

    ranges::sort(entries, ranges::greater(), [](const auto& e) {
        return e.hashes.size();
    });

    const auto target_size = _max_hash_size_in_memory / 3;
    vlog(
      cst_log.trace,
      "Before flush, size in memory {}, target {}",
      _total_size,
      target_size);

    size_t idx = 0;
    while (idx < entries.size() && _total_size >= target_size) {
        _total_size -= sizeof(entries[idx].hashes[0])
                       * entries[idx].hashes.size();

        const auto path = entries[idx].ntp.path();

        // Increment the next filename in the map. This must be done before
        // moving the ntp. Even if the flush fails we have gaps in file names,
        // which is fine because the filenames can be arbitrary and the hash
        // loader will read all files in the NTP dir.
        _ntp_flush_states[entries[idx].ntp].next_file_name += 1;

        co_await flush_ntp_hashes(
          _hash_store_path,
          std::move(entries[idx].ntp),
          std::move(entries[idx].hashes),
          entries[idx].file_name);

        vlog(
          cst_log.trace,
          "Flushed ntp {}, size in memory {}, target {}",
          path,
          _total_size,
          target_size);
        idx += 1;
    }

    vlog(cst_log.trace, "After flush, size in memory {}", _total_size);

    // Put back in all the hashes which were not flushed.
    for (; idx < entries.size(); ++idx) {
        _ntp_flush_states[entries[idx].ntp].hashes = std::move(
          entries[idx].hashes);
    }
}

ss::future<> inventory_consumer::stop() { co_await _gate.close(); }

std::optional<model::ntp>
inventory_consumer::ntp_from_path(std::string_view path) {
    std::string ns;
    std::string tp;
    int pid{};

    if (!RE2::FullMatch(path, path_expr, &ns, &tp, &pid)) {
        return std::nullopt;
    }

    return model::ntp{
      model::ns{ns}, model::topic{tp}, model::partition_id{pid}};
}

} // namespace cloud_storage::inventory
