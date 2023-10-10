// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record.h"
#include "model/record_batch_types.h"
#include "seastarx.h"
#include "storage/disk_log_impl.h"
#include "storage/log_manager.h"
#include "storage/tests/storage_test_fixture.h"
#include "utils/base64.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

using namespace storage;

class storage_fixture_test_base : public storage_test_fixture {};

FIXTURE_TEST(test_compact, storage_fixture_test_base) {
    auto mgr = make_log_manager();
    auto ntp = model::ntp("default", "test", 0);
    auto make_compacted_cfg = [&] {
        auto ntp_cfg = ntp_config(ntp, mgr.config().base_dir);
        auto overrides = ntp_config::default_overrides();
        overrides.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        ntp_cfg.set_overrides(overrides);
        return ntp_cfg;
    };
    auto ntp_cfg = make_compacted_cfg();

    auto log = mgr.manage(std::move(ntp_cfg)).get();
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });

    // Write limited-cardinality records per term.
    append_random_batches<key_limited_random_batch_generator>(
      log, 20, model::term_id{0});
    append_random_batches<key_limited_random_batch_generator>(
      log, 20, model::term_id{1});
    log->flush().get();
    append_random_batches<key_limited_random_batch_generator>(
      log, 20, model::term_id{2});
    log->flush().get();
    log->force_roll(ss::default_priority_class()).get();

    // Collect term info.
    auto get_offsets_per_term = [&] {
        absl::btree_map<model::term_id, model::offset> last_offsets_per_term;
        for (int i = 0; i < 3; i++) {
            auto term = model::term_id{i};
            last_offsets_per_term[term]
              = log->get_term_last_offset(term).value_or(model::offset{});
        }
        return last_offsets_per_term;
    };
    auto terms_before = get_offsets_per_term();

    auto segments_before = log->segment_count();
    auto batches_before = read_and_validate_all_batches(log);
    auto read_batches =
      [&](const ss::circular_buffer<model::record_batch>& batches) {
          int batches_logged = 0;
          absl::btree_map<bytes, model::offset> offset_per_key;
          absl::btree_map<bytes, size_t> num_records_per_key;
          for (const auto& b : batches) {
              if (b.header().type != model::record_batch_type::raft_data) {
                  continue;
              }
              auto iter = model::record_batch_iterator::create(b);
              auto base_offset = b.base_offset();
              while (iter.has_next()) {
                  auto r = iter.next();
                  auto o = base_offset + model::offset_delta{r.offset_delta()};
                  debug("Record {}: {}", o, iobuf_to_base64(r.key()));
                  auto key = iobuf_to_bytes(r.key());
                  num_records_per_key[key]++;
                  if (!offset_per_key.contains(key)) {
                      offset_per_key.emplace(key, o);
                  } else {
                      // Keep only the latest offset per key.
                      offset_per_key[key] = std::max(o, offset_per_key[key]);
                  }
              }
              if (batches_logged++ == 10) {
                  break;
              }
          }
          return std::make_pair(
            std::move(offset_per_key), std::move(num_records_per_key));
      };
    auto [latest_offsets_before, _] = read_batches(batches_before);

    // Compact everything.
    ss::abort_source never_abort;
    auto& disk_log = dynamic_cast<disk_log_impl&>(*log);
    compaction_config cfg(
      disk_log.segments().back()->offsets().base_offset,
      ss::default_priority_class(),
      never_abort);
    disk_log.compact(cfg).get();
    BOOST_REQUIRE_EQUAL(segments_before, log->segment_count());

    auto batches_after = read_and_validate_all_batches(log);
    info("Compacted to {} batches", batches_after.size());

    auto [latest_offsets_after, records_per_key] = read_batches(batches_after);
    auto terms_after = get_offsets_per_term();

    // Check that we meaningully reduced duplicates.
    size_t num_duplicates = 0;
    for (const auto& [key, count] : records_per_key) {
        BOOST_REQUIRE_GT(count, 0);
        num_duplicates += (count - 1);
    }
    // Should be equal to the number of closed segments - 1. The last record of
    // each non-final segment may remain if the entire segment was deduplicated
    // to be empty.
    BOOST_REQUIRE_EQUAL(num_duplicates, segments_before - 2);

    // Terms should be unchanged per segment.
    BOOST_REQUIRE_EQUAL(terms_before.size(), terms_after.size());
    for (const auto& [term, offset] : terms_before) {
        BOOST_REQUIRE_EQUAL(terms_after[term], offset);
    }
    // Other sanity checks.
    BOOST_REQUIRE_EQUAL(
      latest_offsets_before.size(), latest_offsets_after.size());
    BOOST_REQUIRE_LT(batches_after.size(), batches_before.size());
    BOOST_REQUIRE_LE(batches_after.size(), 10);

    // Now restart the log.
    log = nullptr;
    mgr.shutdown(ntp).get();
    ntp_cfg = make_compacted_cfg();
    log = mgr.manage(std::move(ntp_cfg)).get();
    auto terms_restarted = get_offsets_per_term();
    auto batches_restarted = read_and_validate_all_batches(log);
    auto [latest_offsets_restarted, records_per_key_restarted] = read_batches(
      batches_restarted);

    // Check for duplicates.
    num_duplicates = 0;
    for (const auto& [key, count] : records_per_key_restarted) {
        BOOST_REQUIRE_GT(count, 0);
        num_duplicates += (count - 1);
    }
    BOOST_REQUIRE_EQUAL(num_duplicates, segments_before - 2);

    // Check the terms match.
    BOOST_REQUIRE_EQUAL(terms_before.size(), terms_restarted.size());
    for (const auto& [term, offset] : terms_before) {
        BOOST_REQUIRE_EQUAL(terms_restarted[term], offset);
    }
    // Check we have all the keys.
    BOOST_REQUIRE_EQUAL(
      latest_offsets_before.size(), latest_offsets_restarted.size());
}
