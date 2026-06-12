/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"
#include "lsm/core/probe.h"
#include "lsm/db/table_cache.h"
#include "lsm/db/version_edit.h"
#include "lsm/db/version_set.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/block_cache.h"
#include "lsm/sst/builder.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <vector>

namespace lsm::db {

using internal::operator""_seqno;

// Describes an SST to materialize via db_test_base::add_sst.
struct sst_spec {
    internal::file_id id;
    internal::level level;
    std::vector<internal::key> keys;
};

// Common fixture for tests that need a version_set backed by a table_cache and
// persistence layer.
class db_test_base : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;

    explicit db_test_base(
      std::unique_ptr<io::data_persistence> data_persistence
      = io::make_memory_data_persistence())
      : _data_persistence(std::move(data_persistence)) {}

    void TearDown() override {
        _table_cache.close().get();
        _data_persistence->close().get();
        _metadata_persistence->close().get();
    }

    const internal::options& options() { return *_options; }
    db::version_set& version_set() { return *_version_set; }

    // Build a real SST from the spec's keys and register it in the version.
    void add_sst(sst_spec spec) {
        auto writer
          = _data_persistence->open_sequential_writer({.id = spec.id}).get();
        sst::builder builder(std::move(writer), {});
        std::ranges::sort(spec.keys);
        for (const auto& key : spec.keys) {
            builder
              .add(
                key,
                iobuf::from(
                  fmt::format("value for {} on level {}", key, spec.level)))
              .get();
        }
        builder.finish().get();
        size_t file_size = builder.file_size();
        builder.close().get();
        auto edit = _version_set->new_edit();
        auto min_seqno = std::ranges::min_element(
                           spec.keys,
                           std::less<>(),
                           [](internal::key_view k) { return k.seqno(); })
                           ->seqno();
        auto max_seqno = std::ranges::max_element(
                           spec.keys,
                           std::less<>(),
                           [](internal::key_view k) { return k.seqno(); })
                           ->seqno();
        edit->add_file({
          .level = spec.level,
          .file_handle = {.id = spec.id},
          .file_size = file_size,
          .smallest = spec.keys.front(),
          .largest = spec.keys.back(),
          .oldest_seqno = min_seqno,
          .newest_seqno = max_seqno,
        });
        edit->set_last_seqno(max_seqno);
        _version_set->log_and_apply(std::move(edit)).get();
    }

    // Add a file to the version using only metadata (no actual SST file).
    void add_file(
      internal::level level,
      internal::file_id id,
      internal::key smallest,
      internal::key largest,
      uint64_t file_size = 100) {
        auto edit = _version_set->new_edit();
        edit->add_file({
          .level = level,
          .file_handle = {.id = id},
          .file_size = file_size,
          .smallest = smallest,
          .largest = largest,
          .oldest_seqno = 0_seqno,
          .newest_seqno = 0_seqno,
        });
        edit->set_last_seqno(0_seqno);
        _version_set->log_and_apply(std::move(edit)).get();
    }

protected:
    ss::lw_shared_ptr<internal::options> _options
      = ss::make_lw_shared<internal::options>();
    std::unique_ptr<io::data_persistence> _data_persistence;
    std::unique_ptr<io::metadata_persistence> _metadata_persistence
      = io::make_memory_metadata_persistence();
    table_cache _table_cache{
      _data_persistence.get(),
      default_max_entries,
      ss::make_lw_shared<probe>(),
      ss::make_lw_shared<sst::block_cache>(1_MiB, ss::make_lw_shared<probe>())};
    ss::lw_shared_ptr<db::version_set> _version_set
      = ss::make_lw_shared<db::version_set>(
        _metadata_persistence.get(), &_table_cache, _options);
};

} // namespace lsm::db
