// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/lsm.h"

#include "base/vassert.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/db/impl.h"
#include "lsm/db/memtable.h"
#include "ssx/time.h"

#include <seastar/core/coroutine.hh>

#include <stdexcept>
#include <utility>

namespace lsm {

namespace {

compression_type translate_compression(options::compression_type type) {
    switch (type) {
    case options::compression_type::none:
        return compression_type::none;
    case options::compression_type::zstd:
        return compression_type::zstd;
    }
    vunreachable("unknown compression type {}", std::to_underlying(type));
}

ss::lw_shared_ptr<internal::options> translate_options(options opts) {
    if (opts.num_levels < 2) {
        throw std::invalid_argument(
          fmt::format(
            "num_levels must be at least 2, got {}", opts.num_levels));
    }

    if (
      opts.level_zero_stop_writes_trigger
      <= opts.level_zero_slowdown_writes_trigger) {
        throw std::invalid_argument(
          fmt::format(
            "level_zero_stop_writes_trigger ({}) must be greater than "
            "level_zero_slowdown_writes_trigger ({})",
            opts.level_zero_stop_writes_trigger,
            opts.level_zero_slowdown_writes_trigger));
    }

    if (
      opts.sst_filter_period != 0
      && (opts.sst_filter_period & (opts.sst_filter_period - 1)) != 0) {
        throw std::invalid_argument(
          fmt::format(
            "sst_filter_period must be a power of two, got {}",
            opts.sst_filter_period));
    }

    if (
      opts.level_one_compaction_trigger
      >= opts.level_zero_slowdown_writes_trigger) {
        throw std::invalid_argument(
          fmt::format(
            "level_one_compaction_trigger ({}) must be less than "
            "level_zero_slowdown_writes_trigger ({})",
            opts.level_one_compaction_trigger,
            opts.level_zero_slowdown_writes_trigger));
    }

    // Create the internal options
    auto internal_opts = ss::make_lw_shared<internal::options>();

    // Set database epoch
    internal_opts->database_epoch = internal::database_epoch{
      opts.database_epoch};

    // Create level configs based on num_levels, and the level 0 settings.
    auto max_level = internal::level{
      static_cast<uint8_t>(opts.num_levels - 1u)};
    internal_opts->levels = internal::options::make_levels(
      {
        .number = internal::level::min(),
        .max_total_bytes = opts.level_zero_stop_writes_trigger
                           * opts.write_buffer_size,
        .max_file_size = opts.write_buffer_size,
      },
      internal::options::default_level_multipler,
      max_level);
    compression_type c = compression_type::none;
    for (uint8_t i = 0; i < opts.num_levels; ++i) {
        if (i < opts.compression_by_level.size()) {
            c = translate_compression(opts.compression_by_level[i]);
        }
        internal_opts->levels[i].compression = c;
    }

    internal_opts->readonly = opts.readonly;
    internal_opts->compaction_scheduling_group
      = opts.compaction_scheduling_group.value_or(
        ss::current_scheduling_group());
    internal_opts->level_zero_slowdown_writes_trigger
      = opts.level_zero_slowdown_writes_trigger;
    internal_opts->level_zero_stop_writes_trigger
      = opts.level_zero_stop_writes_trigger;
    internal_opts->write_buffer_size = opts.write_buffer_size;
    internal_opts->level_one_compaction_trigger
      = opts.level_one_compaction_trigger;
    internal_opts->max_open_files = opts.max_open_files;
    internal_opts->max_pre_open_fibers = opts.max_pre_open_fibers;
    internal_opts->block_cache_size = opts.block_cache_size;
    internal_opts->sst_block_size = opts.sst_block_size;
    internal_opts->sst_filter_period = opts.sst_filter_period;
    internal_opts->file_deletion_delay = opts.file_deletion_delay;
    internal_opts->probe = std::move(opts.probe);

    return internal_opts;
}

sequence_number from_internal_seqno(internal::sequence_number seqno) {
    return sequence_number(seqno());
}

internal::sequence_number to_internal_seqno(sequence_number seqno) {
    return internal::sequence_number(seqno());
}

} // namespace

iterator::iterator(std::unique_ptr<internal::iterator> impl)
  : _impl(std::move(impl)) {}
iterator::iterator(iterator&&) noexcept = default;
iterator& iterator::operator=(iterator&&) noexcept = default;
iterator::~iterator() noexcept = default;

bool iterator::valid() const { return _impl->valid(); }

ss::future<> iterator::seek_to_first() { return _impl->seek_to_first(); }
ss::future<> iterator::seek_to_last() { return _impl->seek_to_last(); }
ss::future<> iterator::seek(std::string_view target) {
    auto key = internal::key::encode({
      .key = lsm::user_key_view(target),
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    co_await _impl->seek(key);
}
ss::future<> iterator::next() { return _impl->next(); }
ss::future<> iterator::prev() { return _impl->prev(); }
std::string_view iterator::key() { return _impl->key().user_key(); }
sequence_number iterator::seqno() {
    return from_internal_seqno(_impl->key().seqno());
}
iobuf iterator::value() { return _impl->value(); }

database::database(std::unique_ptr<db::impl> impl)
  : _impl(std::move(impl)) {}
database::database(database&&) noexcept = default;
database& database::operator=(database&&) noexcept = default;
database::~database() noexcept = default;

ss::future<database> database::open(options opts, io::persistence p) {
    auto impl = co_await db::impl::open(translate_options(opts), std::move(p));
    co_return database(std::move(impl));
}

ss::future<> database::close() { return _impl->close(); }

std::optional<sequence_number> database::max_persisted_seqno() const {
    return _impl->max_persisted_seqno().transform(
      [](auto seqno) { return from_internal_seqno(seqno); });
}

std::optional<sequence_number> database::max_applied_seqno() const {
    return _impl->max_applied_seqno().transform(
      [](auto seqno) { return from_internal_seqno(seqno); });
}

ss::future<> database::flush(ssx::instant deadline) {
    return _impl->flush(deadline);
}

ss::future<> database::apply(write_batch batch) {
    auto b = std::move(batch._batch);
    co_await _impl->apply(std::move(b));
}

ss::future<std::optional<iobuf>> database::get(std::string_view target) {
    auto key = internal::key::encode({
      .key = lsm::user_key_view(target),
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    auto result = co_await _impl->get(key);
    co_return std::move(result).take_value();
}

ss::future<iterator> database::create_iterator() {
    auto iter = co_await _impl->create_iterator({});
    co_return iterator(std::move(iter));
}

snapshot database::create_snapshot() {
    auto snap = _impl->create_snapshot();
    if (!snap) {
        return {nullptr, _impl.get()};
    }
    return {std::move(*snap), _impl.get()};
}

write_batch database::create_write_batch() { return write_batch{_impl.get()}; }

ss::future<bool> database::refresh() { return _impl->refresh(); }

data_stats database::get_data_stats() const { return _impl->get_data_stats(); }

write_batch::write_batch(db::impl* db)
  : _batch(ss::make_lw_shared<db::memtable>())
  , _db(db) {}
write_batch::write_batch(write_batch&&) noexcept = default;
write_batch& write_batch::operator=(write_batch&&) noexcept = default;
write_batch::~write_batch() noexcept = default;

void write_batch::put(
  std::string_view key, iobuf value, sequence_number seqno) {
    auto k = internal::key::encode({
      .key = lsm::user_key_view(key),
      .seqno = to_internal_seqno(seqno),
      .type = internal::value_type::value,
    });
    _batch->put(std::move(k), std::move(value));
}

void write_batch::remove(std::string_view key, sequence_number seqno) {
    auto k = internal::key::encode({
      .key = lsm::user_key_view(key),
      .seqno = to_internal_seqno(seqno),
      .type = internal::value_type::tombstone,
    });
    _batch->remove(std::move(k));
}

ss::future<std::optional<iobuf>> write_batch::get(std::string_view target) {
    auto key = internal::key::encode({
      .key = lsm::user_key_view(target),
      .seqno = internal::sequence_number::max(),
      .type = internal::value_type::value,
    });
    auto result = _batch->get(key);
    if (result.is_missing()) {
        result = co_await _db->get(key);
    }
    co_return std::move(result).take_value();
}

ss::future<iterator> write_batch::create_iterator() {
    auto iter = co_await _db->create_iterator({.memtable = _batch});
    co_return iterator(std::move(iter));
}

snapshot::snapshot(std::unique_ptr<db::snapshot> snap, db::impl* db)
  : _snap(std::move(snap))
  , _db(db) {}
snapshot& snapshot::operator=(snapshot&&) noexcept = default;
snapshot::snapshot(snapshot&&) noexcept = default;
snapshot::~snapshot() = default;

ss::future<std::optional<iobuf>> snapshot::get(std::string_view target) {
    if (_snap == nullptr) {
        co_return std::nullopt;
    }
    auto key = internal::key::encode({
      .key = lsm::user_key_view(target),
      .seqno = _snap->seqno(),
      .type = internal::value_type::value,
    });
    auto result = co_await _db->get(key);
    co_return std::move(result).take_value();
}

ss::future<iterator> snapshot::create_iterator() {
    if (_snap == nullptr) {
        co_return iterator(internal::iterator::create_empty());
    }
    auto iter = co_await _db->create_iterator({.snapshot = _snap.get()});
    co_return iterator(std::move(iter));
}

} // namespace lsm
