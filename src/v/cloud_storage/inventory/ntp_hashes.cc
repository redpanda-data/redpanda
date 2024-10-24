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

#include "cloud_storage/inventory/ntp_hashes.h"

#include "base/likely.h"
#include "cloud_storage/logger.h"
#include "container/fragmented_vector.h"
#include "hashing/xx.h"
#include "serde/rw/rw.h"
#include "serde/rw/vector.h"
#include "utils/directory_walker.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>

#include <charconv>

namespace {

bool is_valid_hash_file(const ss::sstring& filename) {
    uint64_t v;
    return std::from_chars(
             filename.data(), filename.data() + filename.size(), v)
             .ec
           != std::errc::invalid_argument;
}

} // namespace

namespace cloud_storage::inventory {
ntp_path_hashes::ntp_path_hashes(
  model::ntp ntp, std::filesystem::path hash_store_path)
  : _ntp{std::move(ntp)}
  , _ntp_hashes_path{hash_store_path / std::string_view{_ntp.path()}}
  , _rtc{_as}
  , _ctxlog{cst_log, _rtc, _ntp.path()} {}

ntp_path_hashes::ntp_path_hashes(ntp_path_hashes&& other) noexcept
  : _ntp{std::move(other._ntp)}
  , _ntp_hashes_path{std::move(other._ntp_hashes_path)}
  , _path_hashes{std::move(other._path_hashes)}
  , _possible_collisions{std::move(other._possible_collisions)}
  , _loaded{other._loaded}
  , _rtc{_as}
  , _ctxlog{cst_log, _rtc, _ntp.path()}
  , _gate{std::move(other._gate)} {}

ss::future<bool> ntp_path_hashes::load_hashes() {
    auto h = _gate.hold();
    if (co_await ss::file_exists(_ntp_hashes_path.string())) {
        co_await directory_walker::walk(
          _ntp_hashes_path.string(),
          [this](auto de) { return load_hashes(de); });
        _loaded = true;

        vlog(
          _ctxlog.trace,
          "loaded {} hashes from disk, possible collisions {}",
          _path_hashes.size(),
          _possible_collisions.size());
        co_return true;
    }

    vlog(_ctxlog.warn, "hash dir {} not found", _ntp_hashes_path.string());
    co_return false;
}

ss::future<> ntp_path_hashes::load_hashes(ss::directory_entry de) {
    if (
      de.type.has_value()
      && de.type.value() == ss::directory_entry_type::regular
      && is_valid_hash_file(de.name)) {
        co_return co_await ss::util::with_file_input_stream(
          _ntp_hashes_path / std::string{de.name},
          [this](auto& stream) { return load_hashes(stream); });
    }
}

ss::future<> ntp_path_hashes::load_hashes(ss::input_stream<char>& stream) {
    while (!stream.eof()) {
        auto chunk_size_buffer = co_await stream.read_exactly(sizeof(size_t));
        if (chunk_size_buffer.empty()) {
            break;
        }

        iobuf c;
        c.append(std::move(chunk_size_buffer));
        auto size_parser = iobuf_parser(std::move(c));

        auto chunk_size = serde::read<size_t>(size_parser);
        iobuf chunk;
        chunk.append(co_await stream.read_exactly(chunk_size));

        auto chunk_parser = iobuf_parser{std::move(chunk)};
        auto hashes = serde::read<fragmented_vector<uint64_t>>(chunk_parser);
        vlog(_ctxlog.trace, "read {} path hashes from disk", hashes.size());

        for (auto hash : hashes) {
            auto [_, inserted] = _path_hashes.insert(hash);
            if (unlikely(!inserted)) {
                _possible_collisions.insert(hash);
            }
        }
    }
}

ss::future<> ntp_path_hashes::stop() { co_await _gate.close(); }

lookup_result ntp_path_hashes::exists(const remote_segment_path& path) const {
    vassert(_loaded, "lookup without loading hashes from disk");
    const auto& s = path().string();
    const auto hash = xxhash_64(s.data(), s.size());
    if (unlikely(_possible_collisions.contains(hash))) {
        return lookup_result::possible_collision;
    }

    if (_path_hashes.contains(hash)) {
        return lookup_result::exists;
    }

    return lookup_result::missing;
}

} // namespace cloud_storage::inventory
