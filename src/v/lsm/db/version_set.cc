// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/version_set.h"

#include "absl/container/btree_set.h"
#include "base/vlog.h"
#include "container/chunked_vector.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/logger.h"
#include "lsm/core/internal/merging_iterator.h"
#include "lsm/core/internal/two_level_iterator.h"
#include "lsm/db/file_utils.h"
#include "lsm/db/version_edit.h"
#include "lsm/proto/manifest.proto.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

#include <algorithm>
#include <exception>
#include <memory>

namespace lsm::db {

namespace {

using internal::operator""_level;

// An internal iterator. For a given version/level pair, yields information
// about the files in the level. For a given entry, key() is the largest key
// that occurs in the file, and value()  is an 16-byte value containing the file
// number and file size, both encoded using 64bit fixed encoding.
//
// NOTE: It's up to the user of this class to ensure the files pointer is kept
// alive.
class level_file_num_iterator : public internal::iterator {
public:
    static std::pair<internal::file_handle, uint64_t>
    decode_value(const iobuf& v) {
        // We always know it's a single fragment due to how we allocate and
        // write it in the `level_file_num_iterator`.
        const auto& fragment = *v.begin();
        auto it = fragment.get();
        internal::file_handle handle;
        std::memcpy(&handle.id, it, sizeof(handle.id));
        std::advance(it, sizeof(handle.id));
        std::memcpy(&handle.epoch, it, sizeof(handle.epoch));
        std::advance(it, sizeof(handle.epoch));
        uint64_t file_size = 0;
        std::memcpy(&file_size, it, sizeof(file_size));
        return std::make_pair(handle, file_size);
    }

    explicit level_file_num_iterator(
      chunked_vector<ss::lw_shared_ptr<file_meta_data>>* files)
      : _files(files)
      , _index(files->size()) {}

    bool valid() const override { return _index < _files->size(); }
    ss::future<> seek_to_first() override {
        _index = 0;
        co_return;
    }
    ss::future<> seek_to_last() override {
        _index = _files->empty() ? 0 : _files->size() - 1;
        co_return;
    }
    ss::future<> seek(internal::key_view target) override {
        _index = find_file(*_files, target);
        co_return;
    }
    ss::future<> next() override {
        ++_index;
        co_return;
    }
    ss::future<> prev() override {
        if (_index == 0) {
            _index = _files->size();
        } else {
            --_index;
        }
        co_return;
    }
    internal::key_view key() override { return (*_files)[_index]->largest; }
    iobuf value() override {
        iobuf v;
        auto placeholder = v.reserve(sizeof(uint64_t) * 3);
        const auto& f = (*_files)[_index];
        auto id = std::bit_cast<std::array<char, sizeof(uint64_t)>>(
          f->handle.id);
        placeholder.write(id.data(), id.size());
        auto epoch = std::bit_cast<std::array<char, sizeof(uint64_t)>>(
          f->handle.epoch);
        placeholder.write(epoch.data(), epoch.size());
        auto size = std::bit_cast<std::array<char, sizeof(uint64_t)>>(
          f->file_size);
        placeholder.write(size.data(), size.size());
        return v;
    }

private:
    chunked_vector<ss::lw_shared_ptr<file_meta_data>>* _files;
    uint32_t _index;
    iobuf _value_buf;
};

// An iterator that keeps a `version` pointer reference alive.
//
// This is needed because we use versions that are still alive as
// knowledge of whether a version is safe to GC. Some cases, like L0 iteration,
// can use file iterators directly, so in those cases we need some kind of
// reference to keep L0 files alive.
class version_lifetime_iterator : public internal::iterator {
public:
    explicit version_lifetime_iterator(ss::lw_shared_ptr<version> version)
      : _version(std::move(version)) {}

    bool valid() const override { return false; }

    ss::future<> seek_to_first() override { return ss::now(); }

    ss::future<> seek_to_last() override { return ss::now(); }

    ss::future<> seek(internal::key_view) override { return ss::now(); }

    ss::future<> next() override {
        throw invalid_argument_exception(
          "next() called on version lifetime iterator");
    }

    ss::future<> prev() override {
        throw invalid_argument_exception(
          "prev() called on version lifetime iterator");
    }

    internal::key_view key() override {
        throw invalid_argument_exception(
          "key() called on version lifetime iterator");
    }

    iobuf value() override {
        throw invalid_argument_exception(
          "value() called on version lifetime iterator");
    }

private:
    ss::lw_shared_ptr<version> _version;
};

} // namespace

// A helper class to apply a sequence of edits to a version.
class version_set::builder {
public:
    builder(version_set* vset, ss::lw_shared_ptr<version> base)
      : _vset(vset)
      , _base(std::move(base))
      , _levels(_vset->_options->levels.size()) {}

    void apply(const version_edit& edit) {
        for (internal::level level = 0_level;
             level() < edit._mutations_by_level.size();
             ++level) {
            const auto& mutation = edit._mutations_by_level[level];
            // Update compaction pointer
            if (mutation.compact_pointer) {
                _vset->_compact_pointer[level] = *mutation.compact_pointer;
            } else {
                _vset->_compact_pointer[level] = std::nullopt;
            }
            for (internal::file_handle removed_file : mutation.removed_files) {
                _levels[level].removed_files.insert(removed_file);
            }
            for (const auto& added_file : mutation.added_files) {
                auto copy = ss::make_lw_shared(*added_file);
                copy->allowed_seeks = static_cast<int32_t>(
                  copy->file_size
                  / _vset->_options->seek_compaction_bytes_cost_ratio);
                constexpr static int32_t min_allowed_seeks = 100;
                if (copy->allowed_seeks < min_allowed_seeks) {
                    copy->allowed_seeks = min_allowed_seeks;
                }
                _levels[level].removed_files.erase(copy->handle);
                _levels[level].added_files.insert(copy);
            }
        }
    }

    void save_to(version* v) {
        by_smallest_key cmp;
        for (const auto& level : _vset->_options->levels) {
            // Merge the set of added files with the set of pre-existing file.
            // Drop any deleted files. Store the result in *v.
            const auto& base_files = _base->_files[level.number];
            auto base_iter = base_files.begin();
            auto base_end = base_files.end();
            auto& state = _levels[level.number];
            for (const auto& added_file : state.added_files) {
                // Add all smaller files listed in base_
                auto bpos = std::upper_bound(
                  base_iter, base_end, added_file, cmp);
                for (; base_iter != bpos; ++base_iter) {
                    maybe_add_file(v, level.number, *base_iter);
                }
                maybe_add_file(v, level.number, added_file);
            }
            // Add all remaining base files
            for (; base_iter != base_end; ++base_iter) {
                maybe_add_file(v, level.number, *base_iter);
            }
#ifndef NDEBUG
            if (level.number > 0_level) {
                const auto& files = v->_files[level.number];
                for (uint32_t i = 1; i < files.size(); ++i) {
                    const auto& prev_end = files[i - 1]->largest;
                    const auto& this_begin = files[i]->smallest;
                    dassert(
                      prev_end < this_begin,
                      "overlapping ranges in level {}: {} <= {}",
                      level.number,
                      prev_end,
                      this_begin);
                }
            }
#endif
        }
    }

private:
    void maybe_add_file(
      version* v,
      internal::level level,
      ss::lw_shared_ptr<file_meta_data> file) {
        if (_levels[level].removed_files.contains(file->handle)) {
            return;
        }
        auto& files = v->_files[level];
        if (level > 0_level && !files.empty()) {
            dassert(
              files.back()->largest < file->smallest,
              "expected no overlap between files, got: {} >= {}",
              files.back()->largest,
              file->smallest);
        }
        files.push_back(std::move(file));
    }

    version_set* _vset;
    ss::lw_shared_ptr<version> _base;
    struct level_state {
        chunked_hash_set<internal::file_handle> removed_files;
        absl::btree_set<ss::lw_shared_ptr<file_meta_data>, by_smallest_key>
          added_files;
    };
    absl::FixedArray<level_state> _levels;
};

version::version(ctor, version_set* vset)
  : _vset(vset)
  , _files(_vset->_options->levels.size())
  , _compaction_scores(_vset->_options->levels.size(), 0.0) {}

ss::future<> version::add_iterators(
  chunked_vector<std::unique_ptr<internal::iterator>>* iters) {
    // Merge all level zero files together since they may overlap.
    for (const auto& file : _files[0_level]) {
        auto iter = co_await _vset->_table_cache->create_iterator(
          file->handle, file->file_size);
        iters->push_back(std::move(iter));
    }
    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    int non_empty_levels = 0;
    for (const auto& level : std::span(_vset->_options->levels).subspan(1)) {
        if (_files[level.number].empty()) {
            continue;
        }
        ++non_empty_levels;
        iters->push_back(create_concatenating_iterator(level.number));
    }
    // L0 readers don't capture a reference to the version, but other levels do
    // capture a reference. If we only collected L0 files then we need to add a
    // dummy iterator to capture the reference.
    if (non_empty_levels == 0) {
        iters->push_back(
          std::make_unique<version_lifetime_iterator>(shared_from_this()));
    }
}

bool version::update_stats(const get_stats& stats) {
    if (stats.seek_file) {
        const auto& f = *stats.seek_file;
        --f->allowed_seeks;
        if (f->allowed_seeks <= 0 && !_file_to_compact) {
            _file_to_compact = f;
            _file_to_compact_level = stats.seek_file_level;
            return true;
        }
    }
    return false;
}

ss::future<bool> version::record_read_sample(internal::key_view key) {
    get_stats stats;
    size_t matches = 0;
    co_await for_each_overlapping(
      key,
      [&stats, &matches](
        internal::level level, ss::lw_shared_ptr<file_meta_data> file) {
          ++matches;
          if (matches == 1) {
              stats.seek_file = std::move(file);
              stats.seek_file_level = level;
          }
          return ss::make_ready_future<ss::stop_iteration>(matches >= 2);
      });
    // Must have at least two matches since we want to merge across files. But
    // what if we have a single file that contains many overwrites and
    // deletions? Should we have another mechanism for finding such files?
    if (matches >= 2) {
        // 1MiB cost is about 1 seek (see comment in
        // options::seek_compaction_bytes_cost_ratio).
        co_return update_stats(stats);
    }
    co_return false;
}

chunked_vector<ss::lw_shared_ptr<file_meta_data>>
version::get_overlapping_inputs(
  internal::level level,
  std::optional<user_key_view> begin,
  std::optional<user_key_view> end) {
    chunked_vector<ss::lw_shared_ptr<file_meta_data>> inputs;
    const auto& files = _files[level];
    for (size_t i = 0; i < files.size();) {
        const auto& file = files[i++];
        if (begin && file->largest.user_key() < *begin) {
            // file is completely before specified range; skip it
        } else if (end && file->smallest.user_key() > *end) {
            // file is completely after specified range; skip it
        } else {
            inputs.push_back(file);
            // Level 0 files may overlap each over. So check if the newly added
            // file has expanded the range. If so, restart search.
            if (level == 0_level) {
                if (begin && file->smallest.user_key() < *begin) {
                    begin = file->smallest.user_key();
                    inputs.clear();
                    i = 0;
                } else if (end && file->largest.user_key() > *end) {
                    end = file->largest.user_key();
                    inputs.clear();
                    i = 0;
                }
            }
        }
    }
    return inputs;
}

namespace {

struct found_value {
    internal::key key;
    iobuf value;
};

struct lookup_state {
    std::optional<found_value> found;
    version::get_stats last_file_read;
    version::get_stats* stats;
    table_cache* table_cache;
    internal::key_view target;

    ss::future<ss::stop_iteration>
    on_file(internal::level level, ss::lw_shared_ptr<file_meta_data> file);
};

ss::future<ss::stop_iteration> lookup_state::on_file(
  internal::level level, ss::lw_shared_ptr<file_meta_data> file) {
    if (!stats->seek_file && last_file_read.seek_file) {
        *stats = last_file_read;
    }
    last_file_read.seek_file = file;
    last_file_read.seek_file_level = level;
    co_await table_cache->get(
      file->handle,
      file->file_size,
      target,
      [this](internal::key_view key, iobuf value) {
          if (key.user_key() != target.user_key()) {
              return ss::now();
          }
          found.emplace(internal::key{key}, std::move(value));
          return ss::now();
      });
    co_return found ? ss::stop_iteration::yes : ss::stop_iteration::no;
}

} // namespace

ss::future<lookup_result>
version::get(internal::key_view target, get_stats* stats) {
    stats->seek_file = std::nullopt;
    lookup_state state{
      .stats = stats,
      .table_cache = _vset->_table_cache,
      .target = target,
    };
    co_await for_each_overlapping(
      target,
      [&state](internal::level level, ss::lw_shared_ptr<file_meta_data> file) {
          return state.on_file(level, file);
      });
    if (!state.found) {
        co_return lookup_result::missing();
    } else if (state.found->key.is_tombstone()) {
        co_return lookup_result::tombstone();
    }
    co_return lookup_result::value(std::move(state.found->value));
}

bool version::overlap_in_level(
  internal::level level,
  std::optional<user_key_view> begin,
  std::optional<user_key_view> end) {
    return some_file_overlaps_range(level > 0_level, _files[level], begin, end);
}

internal::level version::pick_level_for_memtable_output(
  user_key_view begin, user_key_view end) {
    auto level = 0_level;
    if (!overlap_in_level(level, begin, end)) {
        // Push to next level if there is no overlap in next level,
        // and the bytes overlapping in the level after that are limited.
        // As we try to skip expensive level 0=>1 compactions if possible
        constexpr static auto max_mem_compact_level = 1_level;
        while (level <= max_mem_compact_level) {
            if (overlap_in_level(level + 1_level, begin, end)) {
                break;
            }
            if (level() + 2 < _vset->_options->levels.size()) {
                // Check that file does not overlap too many grandparent
                // bytes.
                auto files = get_overlapping_inputs(
                  level + 2_level, begin, end);
                size_t sum = total_file_size(files);
                if (
                  sum > _vset->_options->max_grandparent_overlap_bytes(level)) {
                    break;
                }
            }
            ++level;
        }
    }
    return level;
}

std::unique_ptr<internal::iterator>
version::create_concatenating_iterator(internal::level level) {
    auto index_iter = std::make_unique<level_file_num_iterator>(&_files[level]);
    // Keep a strong reference at least in the lambda.
    return internal::create_two_level_iterator(
      std::move(index_iter), [self = shared_from_this()](iobuf value) {
          auto [handle, file_size] = level_file_num_iterator::decode_value(
            value);
          return self->_vset->_table_cache->create_iterator(handle, file_size);
      });
}

ss::future<> version::for_each_overlapping(
  internal::key_view target,
  absl::FunctionRef<ss::future<ss::stop_iteration>(
    internal::level, ss::lw_shared_ptr<file_meta_data>)> fn) {
    // NOTE: it is critical to use user keys to find overlapping files, since
    // the internal keys include sequence numbers and result in incorrect file
    // bound overlap checks.
    auto user_key = target.user_key();
    // Search level-0 from newest to oldest
    chunked_vector<ss::lw_shared_ptr<file_meta_data>> tmp;
    tmp.reserve(_files[0_level].size());
    for (const auto& file : _files[0_level]) {
        if (
          user_key >= file->smallest.user_key()
          && user_key <= file->largest.user_key()) {
            tmp.push_back(file);
        }
    }
    if (!tmp.empty()) {
        std::ranges::sort(
          tmp,
          [](
            const ss::lw_shared_ptr<file_meta_data>& a,
            const ss::lw_shared_ptr<file_meta_data>& b) {
              return a->handle.id > b->handle.id;
          });
        for (const auto& file : tmp) {
            auto stop = co_await fn(0_level, file);
            if (stop == ss::stop_iteration::yes) {
                co_return;
            }
        }
    }
    // Search other levels.
    for (auto level = 1_level; level() < _files.size(); ++level) {
        const auto& files = _files[level];
        if (files.empty()) {
            continue;
        }
        // We binary search on the internal key not user key because
        // if there is a key split across files we need to skip newer
        // key versions.
        size_t index = find_file(files, target);
        if (index < files.size()) {
            const auto& file = files[index];
            if (user_key < file->smallest.user_key()) {
                // All of file is past any data for the key
            } else {
                auto stop = co_await fn(level, file);
                if (stop == ss::stop_iteration::yes) {
                    co_return;
                }
            }
        }
    }
}

void version::for_each_level(
  absl::FunctionRef<void(
    internal::level, const chunked_vector<ss::lw_shared_ptr<file_meta_data>>&)>
    func) const {
    for (auto l = 0_level; l() < _files.size(); ++l) {
        func(l, _files[l]);
    }
}

fmt::iterator version::format_to(fmt::iterator it) const {
    // For example:
    //   --- level 1 ---
    //   0-17:234['a' .. 'e']
    //   3-20:31['a' .. 'e']
    for (size_t level = 0; level < _files.size(); ++level) {
        it = fmt::format_to(it, "--- level {} ---\n", level);
        for (const auto& file : _files[level]) {
            it = fmt::format_to(
              it,
              "{}-{}:{}['{}' .. '{}']\n",
              file->handle.epoch,
              file->handle.id,
              file->file_size,
              file->smallest,
              file->largest);
        }
    }
    return it;
}

version_set::version_set(
  io::metadata_persistence* persistence,
  table_cache* table_cache,
  ss::lw_shared_ptr<internal::options> opts)
  : _persistence(persistence)
  , _table_cache(table_cache)
  , _options(std::move(opts))
  , _compacting_levels(_options->levels.size(), false)
  , _compact_pointer(_options->levels.size()) {
    set_current(ss::make_lw_shared<version>(version::ctor{}, this));
}

void version_set::set_current(ss::lw_shared_ptr<version> new_version) {
    vlog(log.trace, "installing_new_version version=\n{}", *new_version);
    weak_intrusive_list<version>::push_front(&_current, std::move(new_version));
}

ss::lw_shared_ptr<version_edit> version_set::new_edit() {
    auto new_edit = ss::make_lw_shared<version_edit>(this, *_options);
    _live_edits.push_back(*new_edit);
    return new_edit;
}

ss::future<> version_set::log_and_apply(ss::lw_shared_ptr<version_edit> edit) {
    auto v = ss::make_lw_shared<version>(version::ctor{}, this);
    {
        version_set::builder builder(this, _current);
        builder.apply(*edit);
        builder.save_to(v.get());
    }
    finalize(v.get());
    // This is where we diverge a bit from LevelDB. We don't log manifest
    // deltas, but just snapshot the full manifest. At somepoint we will
    // want delta writes (but that's not possible in the cloud), but for
    // now we will just write full snapshots.

    // Invariant: Between the two we must have a seqno, either data exists or
    // we're writing the first data which must have a seqno
    auto updated_seqno = std::max(_last_seqno, edit->_last_seqno).value();
    auto m = manifest{
      .version = v,
      .next_file_id = _next_file_id,
      .last_seqno = updated_seqno,
      .epoch = _options->database_epoch,
    };
    auto fut = co_await ss::coroutine::as_future(write_manifest(std::move(m)));
    if (fut.failed()) {
        std::rethrow_exception(fut.get_exception());
    }
    // Now that the new version is persisted successfully, install the new
    // version
    set_current(std::move(v));
    _last_seqno = updated_seqno;
}

ss::future<> version_set::recover() {
    auto m = co_await read_manifest();
    if (!m) {
        co_return;
    }
    finalize(m->version.get());
    set_current(std::move(m->version));
    _next_file_id = m->next_file_id;
    _last_seqno = m->last_seqno;
}

ss::future<bool> version_set::refresh() {
    auto m = co_await read_manifest();
    if (!m) {
        co_return false;
    }

    if (m->next_file_id < _next_file_id) {
        throw corruption_exception(
          "manifest next_file_id {} is less than current {}",
          m->next_file_id(),
          _next_file_id());
    }
    if (_last_seqno.has_value() && m->last_seqno < _last_seqno.value()) {
        throw corruption_exception(
          "manifest last_seqno {} is less than current {}",
          m->last_seqno(),
          _last_seqno.value()());
    }

    if (m->next_file_id == _next_file_id && m->last_seqno == _last_seqno) {
        co_return false;
    }

    finalize(m->version.get());
    set_current(std::move(m->version));
    _next_file_id = m->next_file_id;
    _last_seqno = m->last_seqno;
    co_return true;
}

void version_set::finalize(version* v) {
    if (_options->readonly) {
        // No need to compute any compaction states, as we won't run compaction
        // in read-only mode.
        return;
    }
    // Precompute the best level for the next compaction
    v->_compaction_scores[0_level]
      = static_cast<double>(v->_files[0_level].size())
        / static_cast<double>(_options->level_one_compaction_trigger);
    // While level 0 compaction score is based on number of files, other levels
    // are based on number of bytes in the level.
    for (auto level = 1_level; level < _options->max_level(); ++level) {
        size_t level_bytes = total_file_size(v->_files[level]);
        double score = static_cast<double>(level_bytes)
                       / static_cast<double>(
                         _options->levels[level].max_total_bytes);
        v->_compaction_scores[level] = score;
    }
}

ss::future<> version_set::write_manifest(manifest m) {
    proto::version version_proto;
    for (const auto& [level, files] :
         std::views::zip(std::views::iota(0), m.version->_files)) {
        proto::version_level level_proto;
        level_proto.set_number(level);
        for (const auto& file : files) {
            proto::file_meta_data file_proto;
            file_proto.set_id(file->handle.id());
            file_proto.set_database_epoch(file->handle.epoch());
            file_proto.set_file_size(file->file_size);
            file_proto.set_encoded_smallest_key(iobuf(file->smallest));
            file_proto.set_encoded_largest_key(iobuf(file->largest));
            file_proto.set_oldest_seqno(file->oldest_seqno());
            file_proto.set_newest_seqno(file->newest_seqno());
            level_proto.get_files().push_back(std::move(file_proto));
        }
        version_proto.get_levels().push_back(std::move(level_proto));
    }
    proto::manifest manifest_proto;
    manifest_proto.set_version(std::move(version_proto));
    manifest_proto.set_next_file_id(m.next_file_id());
    manifest_proto.set_last_seqno(m.last_seqno());
    manifest_proto.set_database_epoch(m.epoch());
    auto serialized = co_await manifest_proto.to_proto();
    co_await _persistence->write_manifest(
      _options->database_epoch, std::move(serialized));
}

ss::future<std::optional<version_set::manifest>> version_set::read_manifest() {
    auto proto = co_await _persistence->read_manifest(_options->database_epoch);
    if (!proto) {
        co_return std::nullopt;
    }
    proto::manifest manifest_proto;
    try {
        manifest_proto = co_await proto::manifest::from_proto(
          std::move(*proto));
    } catch (const std::exception& ex) {
        throw corruption_exception(
          "unable to parse manifest file: {}", ex.what());
    }
    auto v = ss::make_lw_shared<version>(version::ctor{}, this);
    for (const auto& level_proto : manifest_proto.get_version().get_levels()) {
        auto& files = v->_files[internal::level{
          static_cast<uint8_t>(level_proto.get_number())}];
        for (const auto& file_proto : level_proto.get_files()) {
            auto meta = ss::make_lw_shared<file_meta_data>();
            meta->handle.id = internal::file_id{file_proto.get_id()};
            meta->handle.epoch = internal::database_epoch{
              file_proto.get_database_epoch()};
            meta->file_size = file_proto.get_file_size();
            meta->smallest = internal::key(
              file_proto.get_encoded_smallest_key());
            meta->largest = internal::key(file_proto.get_encoded_largest_key());
            meta->oldest_seqno = internal::sequence_number(
              file_proto.get_oldest_seqno());
            meta->newest_seqno = internal::sequence_number(
              file_proto.get_newest_seqno());
            files.push_back(std::move(meta));
        }
    }
    manifest m;
    m.version = std::move(v);
    m.next_file_id = internal::file_id(manifest_proto.get_next_file_id());
    m.last_seqno = internal::sequence_number(manifest_proto.get_last_seqno());
    m.epoch = internal::database_epoch(manifest_proto.get_database_epoch());
    co_return m;
}

bool version_set::needs_compaction() const {
    if (
      _current->_file_to_compact
      && !_compacting_levels[_current->_file_to_compact_level]
      && !_compacting_levels[_current->_file_to_compact_level + 1_level]) {
        return true;
    }
    const auto& scores = _current->_compaction_scores;
    for (auto lvl = 0_level; lvl < _options->max_level(); ++lvl) {
        if (_compacting_levels[lvl] || _compacting_levels[lvl + 1_level]) {
            continue;
        }
        if (scores[lvl] >= 1) {
            return true;
        }
    }
    return false;
}

ss::optimized_optional<std::unique_ptr<compaction>>
version_set::pick_compaction() {
    internal::level level;
    std::unique_ptr<compaction> c;
    using which = compaction::which;
    // We prefer compactions triggered by too much data in a level over
    // compactions triggered by seeks.
    std::optional<internal::level> size_compaction;
    const auto& scores = _current->_compaction_scores;
    for (auto lvl = 0_level; lvl < _options->max_level(); ++lvl) {
        if (_compacting_levels[lvl] || _compacting_levels[lvl + 1_level]) {
            continue;
        }
        double score = scores[lvl];
        if (!size_compaction && score >= 1) { // NOLINT(*-branch-clone)
            size_compaction.emplace(lvl);
        } else if (size_compaction && score > scores[*size_compaction]) {
            size_compaction.emplace(lvl);
        }
    }
    bool seek_compaction
      = _current->_file_to_compact != std::nullopt
        && !_compacting_levels[_current->_file_to_compact_level]
        && !_compacting_levels[_current->_file_to_compact_level + 1_level];
    if (size_compaction) {
        level = size_compaction.value();
        vassert(
          level() + 1 < _options->levels.size(),
          "cannot compact the bottom-most level");
        c = std::make_unique<compaction>(
          compaction::ctor{}, _options, _current, new_edit(), level);
        // Pick the first file that comes after _compact_pointer[level]
        for (const auto& f : _current->_files[level]) {
            const auto& key = _compact_pointer[level];
            if (!key || f->largest > *key) {
                c->_inputs[which::input_level].push_back(f);
                break;
            }
        }
        if (c->_inputs[which::input_level].empty()) {
            // Wrap-around to the beginning of the key space
            c->_inputs[which::input_level].push_back(
              _current->_files[level].front());
        }
    } else if (seek_compaction) {
        level = _current->_file_to_compact_level;
        c = std::make_unique<compaction>(
          compaction::ctor{}, _options, _current, new_edit(), level);
        c->_inputs[which::input_level].push_back(*_current->_file_to_compact);
    } else {
        return std::nullopt;
    }

    // files in level 0 may overlap each other, so pick up all overlapping ones.
    if (level == 0_level) {
        auto [smallest, largest] = get_range(c->_inputs[which::input_level]);
        c->_inputs[which::input_level] = _current->get_overlapping_inputs(
          0_level, smallest.user_key(), largest.user_key());
    }
    add_boundary_inputs(
      _current->_files[level], &c->_inputs[which::input_level]);
    auto [smallest, largest] = get_range(c->_inputs[which::input_level]);
    c->_inputs[which::output_level] = _current->get_overlapping_inputs(
      level + 1_level, smallest.user_key(), largest.user_key());
    add_boundary_inputs(
      _current->_files[level + 1_level], &c->_inputs[which::output_level]);
    // Get entire range covered by compaction
    auto [all_smallest, all_largest] = get_range(
      c->_inputs[which::input_level], c->_inputs[which::output_level]);
    // See if we can grow the number of inputs in "level" without changing the
    // number of "level+1" files we pick up.
    if (!c->_inputs[which::output_level].empty()) {
        auto expanded0 = _current->get_overlapping_inputs(
          level, all_smallest.user_key(), all_largest.user_key());
        add_boundary_inputs(_current->_files[level], &expanded0);
        auto inputs1_size = total_file_size(c->_inputs[which::output_level]);
        auto expanded0_size = total_file_size(expanded0);
        if (
          expanded0.size() > c->_inputs[which::input_level].size()
          && inputs1_size + expanded0_size
               < _options->expanded_compaction_byte_size_limit(level)) {
            auto [new_smallest, new_largest] = get_range(expanded0);
            auto expanded1 = _current->get_overlapping_inputs(
              level + 1_level, new_smallest.user_key(), new_largest.user_key());
            add_boundary_inputs(_current->_files[level + 1_level], &expanded1);
            if (expanded1.size() == c->_inputs[which::output_level].size()) {
                smallest = new_smallest;
                largest = new_largest;
                c->_inputs[which::input_level] = std::move(expanded0);
                c->_inputs[which::output_level] = std::move(expanded1);
                auto new_range = get_range(
                  c->_inputs[which::input_level],
                  c->_inputs[which::output_level]);
                all_smallest = new_range.first;
                all_largest = new_range.second;
            }
        }
    }
    // Compute the set of grandparent files that overlap this compaction.
    if (level() + 2 < _options->levels.size()) {
        c->_grandparents = _current->get_overlapping_inputs(
          level + 2_level, all_smallest.user_key(), all_largest.user_key());
    }
    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the version_edit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    _compact_pointer[level] = largest;
    c->_edit->set_compact_pointer(level, largest);
    vlog(log.trace, "picked_compaction compaction={}", *c);
    return c;
}

ss::future<std::unique_ptr<internal::iterator>>
version_set::make_input_iterator(
  compaction* c, internal::iterator_options iter_opts) {
    // Level 0 files have to be merged together. For other levels, we will make
    // a concatenating iterator per level.
    size_t space = c->level() == 0_level
                     ? c->num_input_files(compaction::which::input_level) + 1
                     : 2;
    chunked_vector<std::unique_ptr<internal::iterator>> list;
    list.reserve(space);
    for (auto& inputs : c->_inputs) {
        if (inputs.empty()) {
            continue;
        }
        if (&inputs == &c->_inputs.front() && c->level() == 0_level) {
            for (auto& file : inputs) {
                // NOTE: in version::add_iterators we have to ensure that L0
                // only iterators keep a ref to their version, in this case we
                // rely on compaction for that.
                list.push_back(
                  co_await _table_cache->create_iterator(
                    file->handle, file->file_size, iter_opts));
            }
        } else {
            auto index_iter = std::make_unique<level_file_num_iterator>(
              &inputs);
            list.push_back(
              internal::create_two_level_iterator(
                std::move(index_iter),
                [self = c->_input_version, iter_opts](iobuf value) {
                    auto [handle, file_size]
                      = level_file_num_iterator::decode_value(value);
                    return self->_vset->_table_cache->create_iterator(
                      handle, file_size, iter_opts);
                }));
        }
    }
    vassert(
      list.size() <= space,
      "expected space to be inclusive of all files: {} <= {}",
      list.size(),
      space);
    co_return internal::create_merging_iterator(std::move(list));
}

chunked_hash_set<internal::file_handle> version_set::get_live_files() {
    chunked_hash_set<internal::file_handle> all_files;
    for (auto v = _current; v != nullptr; v = *v->next()) {
        for (const auto& files : v->_files) {
            for (const auto& file : files) {
                all_files.insert(file->handle);
            }
        }
    }
    return all_files;
}

internal::file_id version_set::min_uncommitted_file_id() const {
    auto min = _next_file_id;
    for (auto& e : _live_edits) {
        min = std::min(min, e._min_allocated_id);
    }
    return min;
}

compaction::compaction(
  ctor,
  ss::lw_shared_ptr<internal::options> options,
  ss::lw_shared_ptr<version> version,
  ss::lw_shared_ptr<version_edit> edit,
  internal::level level)
  : _level(level)
  , _input_version(std::move(version))
  , _edit(std::move(edit))
  , _level_ptrs(/*n=*/options->levels.size(), /*val=*/0) {
    // Mark the input and output levels as having compaction running so we don't
    // try and schedule anything that could conflict.
    auto* vset = _input_version->_vset;
    vset->_compacting_levels[_level] = true;
    vset->_compacting_levels[_level + 1_level] = true;
}

compaction::~compaction() {
    auto* vset = _input_version->_vset;
    vset->_compacting_levels[_level] = false;
    vset->_compacting_levels[_level + 1_level] = false;
}

bool compaction::is_trivial_move() const {
    auto* vset = _input_version->_vset;
    // Avoid a move if there is lots of overlapping grandparent data.
    // Otherwise, the move could create a parent file that will require a very
    // expensive merge later on.
    return (
      num_input_files(which::input_level) == 1
      && num_input_files(which::output_level) == 0
      && total_file_size(_grandparents)
           <= vset->_options->max_grandparent_overlap_bytes(_level));
}

void compaction::add_input_deletions(version_edit* edit) {
    for (uint8_t i = 0; static_cast<size_t>(i) < _inputs.size(); ++i) {
        for (const auto& file : _inputs[i]) { // NOLINT(*bounds-constant-array*)
            edit->remove_file(_level + internal::level{i}, file->handle);
        }
    }
}

bool compaction::is_base_level_for_key(internal::key_view key) {
    const auto& opts = *_input_version->_vset->_options;
    for (auto lvl = _level() + 2u; lvl < opts.levels.size(); ++lvl) {
        const auto& files = _input_version->_files[lvl];
        while (_level_ptrs[lvl] < files.size()) {
            const auto& f = files[_level_ptrs[lvl]];
            if (key.user_key() <= f->largest.user_key()) {
                // We've advanced far enough
                if (key.user_key() >= f->smallest.user_key()) {
                    // Key falls in this file's range, so definitely not base
                    // level.
                    return false;
                }
                break;
            }
            ++_level_ptrs[lvl];
        }
    }
    return true;
}

bool compaction::should_stop_before(internal::key_view key) {
    auto* vset = _input_version->_vset;
    // Scan to find earliest grandparent file that contains key
    while (_grandparent_index < _grandparents.size()
           && key > _grandparents[_grandparent_index]->largest) {
        if (_seen_key) {
            _overlapped_bytes += _grandparents[_grandparent_index]->file_size;
        }
        ++_grandparent_index;
    }
    _seen_key = true;
    if (
      _overlapped_bytes
      > vset->_options->max_grandparent_overlap_bytes(_level)) {
        // Too much overlap for current output; start new output
        _overlapped_bytes = 0;
        return true;
    } else {
        return false;
    }
}

fmt::iterator compaction::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "level={} input_level_files={{{}}} "
      "output_level_files={{{}}} grandparents={{{}}}",
      _level,
      fmt::join(
        std::views::transform(
          _inputs[which::input_level],
          [](const auto& file) { return file->handle; }),
        ","),
      fmt::join(
        std::views::transform(
          _inputs[which::output_level],
          [](const auto& file) { return file->handle; }),
        ","),
      fmt::join(
        std::views::transform(
          _grandparents, [](const auto& file) { return file->handle; }),
        ","));
}

} // namespace lsm::db
