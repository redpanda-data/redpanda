/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

// Tests that assert the Kafka/metastore-correct contract for
// remove_topics_db_update::build_rows: a partition's metadata row must not be
// tombstoned unless every extent row for that partition was successfully
// enumerated and tombstoned too. The metadata row is the only pointer used by
// get_partitions_for_topic to rediscover a partition, so dropping it while
// extent rows survive makes those rows (and the L1 objects they reference)
// permanently unreachable.

#include "bytes/ioarray.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "lsm/core/exceptions.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/io/persistence.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "serde/rw/envelope.h"
#include "serde/rw/rw.h"
#include "ssx/time.h"
#include "test_utils/test.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <exception>
#include <map>

using namespace cloud_topics::l1;

namespace {

// Counts reads through the data persistence layer and injects a single
// io_error_exception on the read whose 1-based index equals `fail_at`.
struct fault_state {
    size_t reads{0};
    size_t fail_at{0};
    bool injected{false};
};

class faulting_reader final : public lsm::io::random_access_file_reader {
public:
    faulting_reader(
      std::unique_ptr<lsm::io::random_access_file_reader> inner,
      fault_state* state)
      : _inner(std::move(inner))
      , _state(state) {}

    ss::future<ioarray> read(size_t offset, size_t n) override {
        ++_state->reads;
        if (_state->fail_at != 0 && _state->reads == _state->fail_at) {
            _state->injected = true;
            return ss::make_exception_future<ioarray>(
              lsm::io_error_exception("injected read failure"));
        }
        return _inner->read(offset, n);
    }

    ss::future<> close() override { return _inner->close(); }

    fmt::iterator format_to(fmt::iterator it) const override {
        return _inner->format_to(it);
    }

private:
    std::unique_ptr<lsm::io::random_access_file_reader> _inner;
    fault_state* _state;
};

class faulting_data_persistence final : public lsm::io::data_persistence {
public:
    faulting_data_persistence(
      std::unique_ptr<lsm::io::data_persistence> inner, fault_state* state)
      : _inner(std::move(inner))
      , _state(state) {}

    ss::future<lsm::io::optional_pointer<lsm::io::random_access_file_reader>>
    open_random_access_reader(
      lsm::internal::file_handle h, uint64_t file_size) override {
        auto reader = co_await _inner->open_random_access_reader(h, file_size);
        if (!reader) {
            co_return lsm::io::optional_pointer<
              lsm::io::random_access_file_reader>{};
        }
        std::unique_ptr<lsm::io::random_access_file_reader> wrapped
          = std::make_unique<faulting_reader>(std::move(*reader), _state);
        co_return wrapped;
    }

    ss::future<std::unique_ptr<lsm::io::sequential_file_writer>>
    open_sequential_writer(lsm::internal::file_handle h) override {
        return _inner->open_sequential_writer(h);
    }

    ss::future<> remove_file(lsm::internal::file_handle h) override {
        return _inner->remove_file(h);
    }

    ss::coroutine::experimental::generator<lsm::internal::file_handle>
    list_files() override {
        auto gen = _inner->list_files();
        while (auto fh = co_await gen()) {
            co_yield fh->get();
        }
    }

    ss::future<> close() override { return _inner->close(); }

private:
    std::unique_ptr<lsm::io::data_persistence> _inner;
    fault_state* _state;
};

const auto topic = model::topic_id(
  uuid_t::from_string("12345678-1234-5678-1234-567812345678"));

model::topic_id_partition tidp(int32_t p) {
    return model::topic_id_partition(topic, model::partition_id(p));
}

struct partition_spec {
    int32_t partition;
    object_id oid;
    kafka::offset base;
    kafka::offset last;
    size_t len;
    int32_t num_terms{0};
};

// What build_rows emitted for a given partition.
struct partition_rows {
    bool metadata_tombstoned{false};
    bool compaction_tombstoned{false};
    std::vector<int64_t> extent_tombstones;
    std::vector<int64_t> term_tombstones;
};

std::map<int32_t, partition_rows>
summarize(const chunked_vector<write_batch_row>& rows) {
    std::map<int32_t, partition_rows> out;
    for (const auto& row : rows) {
        const bool tombstone = row.value.empty();
        if (auto k = metadata_row_key::decode(row.key); k.has_value()) {
            out[k->tidp.partition()].metadata_tombstoned = tombstone;
            continue;
        }
        if (auto k = compaction_row_key::decode(row.key); k.has_value()) {
            out[k->tidp.partition()].compaction_tombstoned = tombstone;
            continue;
        }
        if (auto k = extent_row_key::decode(row.key); k.has_value()) {
            if (tombstone) {
                out[k->tidp.partition()].extent_tombstones.push_back(
                  k->base_offset());
            }
            continue;
        }
        if (auto k = term_row_key::decode(row.key); k.has_value()) {
            if (tombstone) {
                out[k->tidp.partition()].term_tombstones.push_back(k->term());
            }
            continue;
        }
    }
    return out;
}

ss::sstring describe(const std::map<int32_t, partition_rows>& summary) {
    ss::sstring s;
    for (const auto& [pid, pr] : summary) {
        ss::sstring offsets;
        for (auto o : pr.extent_tombstones) {
            offsets += fmt::format("{},", o);
        }
        ss::sstring terms;
        for (auto t : pr.term_tombstones) {
            terms += fmt::format("{},", t);
        }
        s += fmt::format(
          "p{}{{metadata_tombstone={} compaction_tombstone={} "
          "extent_tombstones=[{}] term_tombstones=[{}]}} ",
          pid,
          pr.metadata_tombstoned,
          pr.compaction_tombstoned,
          offsets,
          terms);
    }
    return s;
}

} // namespace

class RemoveTopicsReadErrorTest : public ::testing::Test {
protected:
    void SetUp() override { open_db(); }

    void TearDown() override {
        if (db_) {
            db_->close().get();
        }
    }

    // A one-byte block size puts every row in its own SST block, so each row
    // read is a distinct persistence-layer read that the fault sweep can
    // target individually.
    void open_db(size_t sst_block_size = lsm::options::default_sst_block_size) {
        fault_ = std::make_unique<fault_state>();
        db_ = lsm::database::open(
                {.database_epoch = 0, .sst_block_size = sst_block_size},
                lsm::io::persistence{
                  .data = std::make_unique<faulting_data_persistence>(
                    lsm::io::make_memory_data_persistence(), fault_.get()),
                  .metadata = lsm::io::make_memory_metadata_persistence(),
                })
                .get();
    }

    lsm::sequence_number next_seqno() {
        auto max_applied = db_->max_applied_seqno();
        if (!max_applied) {
            return lsm::sequence_number(1);
        }
        return lsm::sequence_number{max_applied.value()() + 1};
    }

    // Writes a complete, consistent set of rows for each partition: metadata,
    // one extent, a compaction row, and the object row the extent lives in.
    void populate(const std::vector<partition_spec>& specs) {
        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& s : specs) {
            auto tp = tidp(s.partition);
            wb.put(
              metadata_row_key::encode(tp),
              serde::to_iobuf(
                metadata_row_value{
                  .start_offset = s.base,
                  .next_offset = kafka::next_offset(s.last),
                  .compaction_epoch = partition_state::compaction_epoch_t{0},
                  .size = s.len,
                  .num_extents = 1,
                }),
              seqno);
            wb.put(
              extent_row_key::encode(tp, s.base),
              serde::to_iobuf(
                extent_row_value{
                  .last_offset = s.last,
                  .max_timestamp = model::timestamp(1000),
                  .filepos = 0,
                  .len = s.len,
                  .oid = s.oid,
                }),
              seqno);
            for (int32_t t = 0; t < s.num_terms; ++t) {
                wb.put(
                  term_row_key::encode(tp, model::term_id(t)),
                  serde::to_iobuf(
                    term_row_value{.term_start_offset = kafka::offset(0)}),
                  seqno);
            }
            wb.put(
              compaction_row_key::encode(tp),
              serde::to_iobuf(compaction_row_value{}),
              seqno);
            wb.put(
              object_row_key::encode(s.oid),
              serde::to_iobuf(
                object_row_value{
                  .object = object_entry{
                    .total_data_size = s.len,
                    .removed_data_size = 0,
                    .footer_pos = 0,
                    .object_size = s.len,
                    .last_updated = model::timestamp(1000),
                    .is_preregistration = false,
                  }}),
              seqno);
        }
        db_->apply(std::move(wb)).get();
    }

    void apply_rows(const chunked_vector<write_batch_row>& rows) {
        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    std::unique_ptr<fault_state> fault_;
    std::optional<lsm::database> db_;
};

// A single extent row whose value cannot be decoded makes the state_reader
// surface an error mid-iteration. build_rows must refuse to tombstone that
// partition's metadata row -- otherwise the surviving extent row becomes
// unreachable forever.
TEST_F(RemoveTopicsReadErrorTest, UndecodableExtentValueMustFailBuildRows) {
    auto oid0 = object_id(uuid_t::create());
    auto oid1 = object_id(uuid_t::create());
    populate({
      {.partition = 0,
       .oid = oid0,
       .base = kafka::offset(0),
       .last = kafka::offset(99),
       .len = 1024},
      {.partition = 1,
       .oid = oid1,
       .base = kafka::offset(0),
       .last = kafka::offset(99),
       .len = 2048},
    });

    // Overwrite partition 1's extent row value with a serde envelope header
    // that claims compat_version=255, which extent_row_value cannot decode.
    {
        iobuf bad;
        const char bytes[] = {'\xff', '\xff', '\x00', '\x00', '\x00', '\x00'};
        bad.append(bytes, sizeof(bytes));
        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        wb.put(
          extent_row_key::encode(tidp(1), kafka::offset(0)),
          std::move(bad),
          seqno);
        db_->apply(std::move(wb)).get();
    }

    // Sanity check: the reader really does report an error for partition 1,
    // and the sibling discovery pass propagates it.
    {
        auto reader = state_reader(db_->create_snapshot());
        auto extents
          = reader.get_inclusive_extents(tidp(1), std::nullopt, std::nullopt)
              .get();
        ASSERT_TRUE(extents.has_value());
        ASSERT_TRUE(extents->has_value());
        auto rows = extents->value().materialize_rows().get();
        ASSERT_EQ(rows.size(), 1);
        ASSERT_FALSE(rows[0].has_value())
          << "expected the corrupt extent value to surface a read error";
        fmt::print(
          "reader error for p1: {}\n", fmt::format("{}", rows[0].error()));
    }
    {
        auto reader = state_reader(db_->create_snapshot());
        auto update = remove_topics_db_update{
          .topics = chunked_vector<model::topic_id>::single(topic),
        };
        auto discovered = update.discover_object_ids(reader).get();
        EXPECT_FALSE(discovered.has_value())
          << "discover_object_ids is expected to propagate the read error";
    }

    auto reader = state_reader(db_->create_snapshot());
    auto update = remove_topics_db_update{
      .topics = chunked_vector<model::topic_id>::single(topic),
    };
    chunked_vector<write_batch_row> rows;
    auto result = update.build_rows(reader, rows).get();

    auto summary = summarize(rows);
    fmt::print(
      "build_rows returned {}; rows={}\n",
      result.has_value() ? "SUCCESS" : "ERROR",
      describe(summary));

    // Correct behaviour: build_rows must not report success when it could not
    // enumerate a partition's extents.
    EXPECT_FALSE(result.has_value())
      << "build_rows reported success despite a read error while enumerating "
         "partition 1's extents; emitted rows: "
      << describe(summary);

    // Correct behaviour: no partition may lose its metadata row while its
    // extent rows survive.
    EXPECT_FALSE(
      summary[1].metadata_tombstoned && summary[1].extent_tombstones.empty())
      << "partition 1's metadata row was tombstoned with no extent tombstones; "
      << describe(summary);

    if (!result.has_value()) {
        return;
    }

    // Demonstrate the permanence claim: apply what build_rows produced and
    // check whether the extent row and its object are still reachable.
    apply_rows(rows);
    auto post = state_reader(db_->create_snapshot());
    auto partitions = post.get_partitions_for_topic(topic).get();
    ASSERT_TRUE(partitions.has_value());
    fmt::print(
      "after applying rows, get_partitions_for_topic returned {} partitions\n",
      partitions->size());

    auto surviving
      = db_->get(extent_row_key::encode(tidp(1), kafka::offset(0))).get();
    fmt::print(
      "extent row for p1 still present after removal: {}\n",
      surviving.has_value());

    auto obj = post.get_object(oid1).get();
    ASSERT_TRUE(obj.has_value());
    if (obj->has_value()) {
        fmt::print(
          "object {} total_data_size={} removed_data_size={} (GC requires "
          "equality)\n",
          oid1,
          obj->value().total_data_size,
          obj->value().removed_data_size);
    }

    EXPECT_TRUE(partitions->empty() == false || surviving.has_value() == false)
      << "partition 1 is no longer discoverable yet its extent row survives";
}

// Sweeps a single injected object-storage read failure across every read the
// removal performs. For every position, build_rows must either report an error
// or emit a complete, self-consistent tombstone set. It must never report
// success having tombstoned a partition's metadata row while leaving that
// partition's extent rows behind.
TEST_F(RemoveTopicsReadErrorTest, TransientReadErrorMustNotOrphanExtentRows) {
    struct outcome {
        size_t fail_at{0};
        bool build_succeeded{false};
        bool threw{false};
        std::map<int32_t, partition_rows> summary;
    };

    auto run = [this](size_t fail_at) -> outcome {
        auto oid0 = object_id(uuid_t::create());
        auto oid1 = object_id(uuid_t::create());
        populate({
          {.partition = 0,
           .oid = oid0,
           .base = kafka::offset(0),
           .last = kafka::offset(99),
           .len = 1024},
          {.partition = 1,
           .oid = oid1,
           .base = kafka::offset(0),
           .last = kafka::offset(99),
           .len = 2048},
        });
        // Push the rows out of the memtable and into an SST so reads go
        // through the (faulting) data persistence layer.
        db_->flush(ssx::instant::infinite_future()).get();

        outcome o;
        o.fail_at = fail_at;
        fault_->reads = 0;
        fault_->injected = false;
        fault_->fail_at = fail_at;

        auto reader = state_reader(db_->create_snapshot());
        auto update = remove_topics_db_update{
          .topics = chunked_vector<model::topic_id>::single(topic),
        };
        chunked_vector<write_batch_row> rows;
        try {
            auto result = update.build_rows(reader, rows).get();
            fault_->fail_at = 0;
            o.build_succeeded = result.has_value();
            o.summary = summarize(rows);
        } catch (const std::exception& e) {
            fault_->fail_at = 0;
            o.threw = true;
            fmt::print(
              "fail_at={}: build_rows threw out of the coroutine: {}\n",
              fail_at,
              e.what());
        }
        return o;
    };

    // First, count the reads a clean removal performs.
    TearDown();
    open_db(/*sst_block_size=*/1);
    size_t total_reads = 0;
    {
        auto o = run(0);
        total_reads = fault_->reads;
        ASSERT_FALSE(o.threw);
        ASSERT_TRUE(o.build_succeeded);
        fmt::print(
          "clean run: reads={} rows={}\n", total_reads, describe(o.summary));
        ASSERT_GT(total_reads, 0u)
          << "no reads reached the persistence layer; the fault sweep would be "
             "vacuous";
        ASSERT_EQ(o.summary.size(), 2u);
        for (auto& [pid, pr] : o.summary) {
            ASSERT_TRUE(pr.metadata_tombstoned);
            ASSERT_EQ(pr.extent_tombstones.size(), 1u) << "partition " << pid;
        }
    }

    std::vector<outcome> violations;
    for (size_t n = 1; n <= total_reads; ++n) {
        TearDown();
        open_db(/*sst_block_size=*/1);
        auto o = run(n);
        if (!fault_->injected) {
            continue;
        }
        if (!o.build_succeeded) {
            // Correct behaviour: the error was propagated.
            continue;
        }
        bool orphaned = false;
        for (const auto& [pid, pr] : o.summary) {
            if (pr.metadata_tombstoned && pr.extent_tombstones.empty()) {
                orphaned = true;
            }
        }
        if (orphaned) {
            violations.push_back(o);
        }
    }

    for (const auto& v : violations) {
        fmt::print(
          "VIOLATION fail_at={}: build_rows returned SUCCESS with rows={}\n",
          v.fail_at,
          describe(v.summary));
    }
    EXPECT_TRUE(violations.empty())
      << violations.size() << " of " << total_reads
      << " single-read-failure positions produced a successful build_rows that "
         "tombstoned a partition's metadata row while leaving its extent rows "
         "in place";
}

// Not a bug test. Measures how much of the fault window survives the LSM block
// cache: db_domain_manager::remove_topics scans the same extent rows twice
// (count_topic_extents, then discover_object_ids) before build_rows runs, and
// both of those propagate errors. If those earlier scans left every block that
// build_rows needs in the block cache, build_rows would perform zero
// persistence reads and the swallowed-error path would be unreachable without
// cache eviction. This records the actual number.
TEST_F(RemoveTopicsReadErrorTest, MeasureReadsLeftForBuildRowsAfterWarmup) {
    auto oid0 = object_id(uuid_t::create());
    auto oid1 = object_id(uuid_t::create());
    populate({
      {.partition = 0,
       .oid = oid0,
       .base = kafka::offset(0),
       .last = kafka::offset(99),
       .len = 1024},
      {.partition = 1,
       .oid = oid1,
       .base = kafka::offset(0),
       .last = kafka::offset(99),
       .len = 2048},
    });
    db_->flush(ssx::instant::infinite_future()).get();

    auto update = remove_topics_db_update{
      .topics = chunked_vector<model::topic_id>::single(topic),
    };

    // Pass 1 and pass 2, each on its own snapshot, exactly as
    // db_domain_manager::remove_topics does.
    fault_->reads = 0;
    {
        auto r = state_reader(db_->create_snapshot());
        ASSERT_TRUE(update.discover_object_ids(r).get().has_value());
    }
    auto reads_pass1 = fault_->reads;
    {
        auto r = state_reader(db_->create_snapshot());
        ASSERT_TRUE(update.discover_object_ids(r).get().has_value());
    }
    auto reads_pass2 = fault_->reads - reads_pass1;

    auto reader = state_reader(db_->create_snapshot());
    chunked_vector<write_batch_row> rows;
    ASSERT_TRUE(update.build_rows(reader, rows).get().has_value());
    auto reads_build_rows = fault_->reads - reads_pass1 - reads_pass2;

    fmt::print(
      "persistence reads: pass1(discover)={} pass2(discover)={} "
      "build_rows={}\n",
      reads_pass1,
      reads_pass2,
      reads_build_rows);
}

// The sharpest reachability test. db_domain_manager::remove_topics scans the
// same rows twice before build_rows (count_topic_extents, then
// discover_object_ids), and both propagate errors, so the block cache is warm
// by the time build_rows runs. This test reproduces that warm-up and then
// injects a single read failure only at positions that occur *after* it, i.e.
// only inside build_rows. If any such position yields a successful build_rows
// with an incomplete tombstone set, the swallowed-error path is reachable in
// production without needing block-cache eviction between the passes.
TEST_F(RemoveTopicsReadErrorTest, WarmCacheReadErrorMustNotOrphanRows) {
    auto oid0 = object_id(uuid_t::create());
    auto oid1 = object_id(uuid_t::create());
    auto specs = std::vector<partition_spec>{
      {.partition = 0,
       .oid = oid0,
       .base = kafka::offset(0),
       .last = kafka::offset(99),
       .len = 1024,
       .num_terms = 2},
      {.partition = 1,
       .oid = oid1,
       .base = kafka::offset(0),
       .last = kafka::offset(99),
       .len = 2048,
       .num_terms = 2},
    };

    struct outcome {
        size_t fail_at{0};
        bool build_succeeded{false};
        bool threw{false};
        std::map<int32_t, partition_rows> summary;
    };

    // Populates, flushes, replays the two error-propagating warm-up scans, then
    // runs build_rows with a read failure armed at absolute read index
    // `fail_at` (0 disarms).
    auto run = [this, &specs](size_t fail_at) -> std::pair<outcome, size_t> {
        populate(specs);
        db_->flush(ssx::instant::infinite_future()).get();

        auto update = remove_topics_db_update{
          .topics = chunked_vector<model::topic_id>::single(topic),
        };
        fault_->reads = 0;
        fault_->injected = false;
        fault_->fail_at = 0;
        for (int i = 0; i < 2; ++i) {
            auto r = state_reader(db_->create_snapshot());
            auto res = update.discover_object_ids(r).get();
            EXPECT_TRUE(res.has_value()) << "warm-up scan " << i << " failed";
        }
        auto reads_after_warmup = fault_->reads;

        outcome o;
        o.fail_at = fail_at;
        fault_->fail_at = fail_at;
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        try {
            auto result = update.build_rows(reader, rows).get();
            fault_->fail_at = 0;
            o.build_succeeded = result.has_value();
            o.summary = summarize(rows);
        } catch (const std::exception& e) {
            fault_->fail_at = 0;
            o.threw = true;
            fmt::print(
              "fail_at={}: build_rows threw out of the coroutine: {}\n",
              fail_at,
              e.what());
        }
        return {o, reads_after_warmup};
    };

    // Measure how many reads build_rows still needs once the cache is warm.
    TearDown();
    open_db(/*sst_block_size=*/1);
    size_t warmup_reads = 0;
    size_t build_rows_reads = 0;
    {
        auto [o, after_warmup] = run(0);
        ASSERT_FALSE(o.threw);
        ASSERT_TRUE(o.build_succeeded);
        warmup_reads = after_warmup;
        build_rows_reads = fault_->reads - after_warmup;
        fmt::print(
          "warm-cache clean run: warmup_reads={} build_rows_reads={} rows={}\n",
          warmup_reads,
          build_rows_reads,
          describe(o.summary));
    }
    ASSERT_GT(build_rows_reads, 0u)
      << "the two warm-up scans left build_rows with no persistence reads at "
         "all, so no read failure can occur inside build_rows";

    std::vector<outcome> violations;
    for (size_t n = warmup_reads + 1; n <= warmup_reads + build_rows_reads;
         ++n) {
        TearDown();
        open_db(/*sst_block_size=*/1);
        auto [o, _] = run(n);
        if (!fault_->injected) {
            continue;
        }
        if (!o.build_succeeded) {
            continue;
        }
        bool incomplete = false;
        for (const auto& [pid, pr] : o.summary) {
            if (!pr.metadata_tombstoned) {
                continue;
            }
            if (
              pr.extent_tombstones.empty() || pr.term_tombstones.size() != 2) {
                incomplete = true;
            }
        }
        if (incomplete) {
            violations.push_back(o);
        }
    }

    for (const auto& v : violations) {
        fmt::print(
          "WARM-CACHE VIOLATION fail_at={}: build_rows returned SUCCESS with "
          "rows={}\n",
          v.fail_at,
          describe(v.summary));
    }
    EXPECT_TRUE(violations.empty())
      << violations.size() << " of " << build_rows_reads
      << " read-failure positions inside build_rows (after the two "
         "error-propagating warm-up scans) produced a successful build_rows "
         "that tombstoned a partition's metadata row without tombstoning all "
         "of its extent and term rows";
}
