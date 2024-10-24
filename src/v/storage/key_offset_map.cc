/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "storage/key_offset_map.h"

namespace storage {

simple_key_offset_map::simple_key_offset_map(std::optional<size_t> max_keys)
  : _memory_tracker(ss::make_shared<util::mem_tracker>("simple_key_offset_map"))
  , _map(util::mem_tracked::map<absl::btree_map, compaction_key, model::offset>(
      _memory_tracker))
  , _max_keys(max_keys ? *max_keys : default_key_limit) {}

seastar::future<bool>
simple_key_offset_map::put(const compaction_key& key, model::offset o) {
    auto iter = _map.find(key);
    if (iter == _map.end()) {
        if (_map.size() >= _max_keys) {
            return seastar::make_ready_future<bool>(false);
        }
        _map[key] = o;
    } else {
        _map[key] = std::max(o, iter->second);
    }
    _max_offset = std::max(_max_offset, o);
    return seastar::make_ready_future<bool>(true);
}

seastar::future<std::optional<model::offset>>
simple_key_offset_map::get(const compaction_key& key) const {
    auto iter = _map.find(key);
    if (iter == _map.end()) {
        return seastar::make_ready_future<std::optional<model::offset>>(
          std::nullopt);
    }
    return seastar::make_ready_future<std::optional<model::offset>>(
      iter->second);
}

model::offset simple_key_offset_map::max_offset() const { return _max_offset; }

size_t simple_key_offset_map::size() const { return _map.size(); }
size_t simple_key_offset_map::capacity() const { return _max_keys; }

seastar::future<std::optional<model::offset>>
hash_key_offset_map::get(const compaction_key& key) const {
    const auto hash = hash_key(key);

    // handle a non-normalized probe position
    // (true, value) -> stop and return value
    // (false, _)    -> keep probing
    auto handle_entry =
      [&](size_t index) -> std::pair<bool, std::optional<model::offset>> {
        ++probe_count_;
        index = index % entries_.size();
        auto& entry = entries_[index];
        if (entry.empty()) {
            return std::make_pair(true, std::nullopt);
        } else if (entry.digest == hash) {
            return std::make_pair(true, entry.offset);
        }
        return std::make_pair(false, std::nullopt);
    };

    ++search_count_;

    // probe using the hash
    probe probe(hash);
    auto index = probe.next();
    while (index.has_value()) {
        const auto [stop, value] = handle_entry(index.value());
        if (stop) {
            return seastar::make_ready_future<std::optional<model::offset>>(
              value);
        }
        const auto next_index = probe.next();
        if (next_index.has_value()) {
            index = next_index;
        } else {
            break;
        }
    }

    // fall back to linear probe
    const auto linear_base = index.value_or(0);
    for (size_t probe = 0; probe < entries_.size(); ++probe) {
        const auto [stop, value] = handle_entry(linear_base + probe);
        if (stop) {
            return seastar::make_ready_future<std::optional<model::offset>>(
              value);
        }
    }

    return seastar::make_ready_future<std::optional<model::offset>>(
      std::nullopt);
}

seastar::future<bool>
hash_key_offset_map::put(const compaction_key& key, model::offset offset) {
    const auto hash = hash_key(key);

    const auto full = size_ >= capacity_;

    enum class handle {
        inserted,
        not_inserted,
        not_inserted_full,
    };

    // handle a non-normalized probe position
    auto handle_entry = [&](size_t index) {
        ++probe_count_;
        index = index % entries_.size();
        auto& entry = entries_[index];
        if (entry.empty()) {
            if (full) {
                return handle::not_inserted_full;
            }
            entry.digest = hash;
            entry.offset = offset;
            ++size_;
            max_offset_ = std::max(max_offset_, offset);
            return handle::inserted;
        } else if (entry.digest == hash) {
            if (offset > entry.offset) {
                entry.offset = offset;
                max_offset_ = std::max(max_offset_, offset);
            }
            return handle::inserted;
        }
        return handle::not_inserted;
    };

    ++search_count_;

    // probe using the hash
    probe probe(hash);
    auto index = probe.next();
    while (index.has_value()) {
        const auto res = handle_entry(index.value());
        if (res == handle::inserted) {
            return seastar::make_ready_future<bool>(true);
        }
        if (res == handle::not_inserted_full) {
            return seastar::make_ready_future<bool>(false);
        }
        const auto next_index = probe.next();
        if (next_index.has_value()) {
            index = next_index;
        } else {
            break;
        }
    }

    // fall back to linear probe
    const auto linear_base = index.value_or(0);
    for (size_t probe = 0; probe < entries_.size(); ++probe) {
        const auto res = handle_entry(linear_base + probe);
        if (res == handle::inserted) {
            return seastar::make_ready_future<bool>(true);
        }
        if (res == handle::not_inserted_full) {
            return seastar::make_ready_future<bool>(false);
        }
    }

    return seastar::make_ready_future<bool>(false);
}

model::offset hash_key_offset_map::max_offset() const { return max_offset_; }

size_t hash_key_offset_map::size() const { return size_; }
size_t hash_key_offset_map::capacity() const { return capacity_; }

seastar::future<> hash_key_offset_map::initialize(size_t size_bytes) {
    co_await fragmented_vector_clear_async(entries_);
    while (entries_.memory_size() < size_bytes) {
        for (size_t i = 0; i < entries_.elements_per_fragment(); ++i) {
            entries_.push_back(entry{});
        }
        if (seastar::need_preempt()) {
            co_await seastar::maybe_yield();
        }
    }
    size_ = 0;
    max_offset_ = model::offset{};
    if (entries_.size() > 0) {
        capacity_ = std::max(
          size_t(1),
          static_cast<size_t>(
            static_cast<double>(entries_.size()) * max_load_factor));
    } else {
        capacity_ = 0;
    }
    search_count_ = 0;
    probe_count_ = 0;
}

seastar::future<> hash_key_offset_map::reset() {
    co_await fragmented_vector_fill_async(entries_, entry{});
    size_ = 0;
    max_offset_ = model::offset{};
    search_count_ = 0;
    probe_count_ = 0;
}

double hash_key_offset_map::hit_rate() const {
    if (probe_count_ == 0) {
        return 1.0;
    }
    return static_cast<double>(search_count_)
           / static_cast<double>(probe_count_);
}

bool hash_key_offset_map::entry::empty() const {
    return digest == hash_type::digest_type{};
}

hash_key_offset_map::probe::probe(const hash_type::digest_type& hash)
  : iter(hash.data())
  , end(iter + hash_type::digest_size) {}

std::optional<hash_key_offset_map::probe::index_type>
hash_key_offset_map::probe::next() {
    if ((iter + sizeof(index_type)) > end) {
        return std::nullopt;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    index_type index;
    std::memcpy(&index, iter, sizeof(index_type));

    iter += sizeof(index_type);

    return index;
}

hash_key_offset_map::hash_type::digest_type
hash_key_offset_map::hash_key(const compaction_key& key) const {
    try {
        hasher_.update(key);
        return hasher_.reset();
    } catch (...) {
        hasher_.reset();
        throw;
    }
}

} // namespace storage
