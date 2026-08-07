// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/errc.h"
#include "cluster/partition.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tx_protocol_types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/txn_offset_commit.h"
#include "kafka/server/group_metadata.h"
#include "kafka/server/offset_store.h"
#include "kafka/server/offset_writer.h"
#include "kafka/server/tx_coordinator_client.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/errc.h"
#include "storage/record_batch_builder.h"
#include "test_utils/scoped_config.h"
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>

using namespace std::chrono_literals;
using namespace kafka;

namespace {

const group_id test_group{"test-group"};

model::topic_partition tp(const ss::sstring& topic, int32_t partition) {
    return {model::topic(topic), model::partition_id(partition)};
}

offset_store::offset_metadata committed(
  int64_t log_offset,
  int64_t offset,
  model::timestamp commit_ts = model::timestamp::now()) {
    return {
      .log_offset = model::offset(log_offset),
      .offset = model::offset(offset),
      .metadata = "",
      .committed_leader_epoch = kafka::leader_epoch(1),
      .commit_timestamp = commit_ts,
    };
}

/// A transaction with plenty of time left on its deadline.
offset_store::ongoing_transaction live_tx(model::tx_seq seq) {
    return offset_store::ongoing_transaction(
      seq, model::partition_id(0), 30s, model::offset(100));
}

/// A transaction whose deadline has already passed.
offset_store::ongoing_transaction expired_tx(model::tx_seq seq) {
    return offset_store::ongoing_transaction(
      seq, model::partition_id(0), 0ms, model::offset(100));
}

offset_store::pending_tx_offset
staged_offset(const model::topic_partition& tp, model::offset offset) {
    return offset_store::pending_tx_offset{
      .offset_metadata = group_tx::partition_offset{
        .tp = tp,
        .offset = offset,
        .leader_epoch = kafka::leader_epoch(1),
        .metadata = std::nullopt,
      },
      .log_offset = model::offset(101),
    };
}

offset_store::ongoing_transaction
staged_tx(model::tx_seq seq, const model::topic_partition& staged_for) {
    auto tx = live_tx(seq);
    tx.offsets[staged_for] = staged_offset(staged_for, model::offset(42));
    return tx;
}

/// Stand-ins for the retention policy the group supplies.
bool all_subscribed(const model::topic&) { return true; }
bool none_subscribed(const model::topic&) { return false; }
model::timestamp expires_at_commit(const offset_store::offset_metadata& md) {
    return md.commit_timestamp;
}

/// Records what the store tried to write and returns whatever the test asked
/// for, so the write paths run without a partition.
class recording_writer final : public offset_writer {
public:
    model::term_id term() const final { return partition_term; }

    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch batch, model::term_id term) final {
        written.push_back(std::move(batch));
        terms.push_back(term);
        ++replicates;
        return deferred_result();
    }

    ss::future<result<raft::replicate_result>> replicate(
      chunked_vector<model::record_batch> batches, model::term_id term) final {
        for (auto& batch : batches) {
            written.push_back(std::move(batch));
        }
        terms.push_back(term);
        ++replicates;
        return deferred_result();
    }

    raft::replicate_stages replicate_in_stages(
      chunked_vector<model::record_batch> batches, model::term_id term) final {
        for (auto& batch : batches) {
            written.push_back(std::move(batch));
        }
        terms.push_back(term);
        ++replicates;
        return raft::replicate_stages(ss::now(), deferred_result());
    }

    ss::future<> maybe_step_down(model::term_id, std::string_view) final {
        ++step_downs;
        return ss::now();
    }

    model::term_id partition_term{1};
    /// The log offset the first write lands at; each one after it lands one
    /// further on, the way raft assigns them.
    model::offset commit_at{42};
    int replicates{0};
    std::optional<std::error_code> fail_with;
    chunked_vector<model::record_batch> written;
    std::vector<model::term_id> terms;
    int step_downs{0};

private:
    /// A real write never resolves before it is returned. Deferring keeps the
    /// store's completion continuation off the caller's stack: seastar runs a
    /// continuation on an already available future inline unless SEASTAR_DEBUG
    /// is defined, which release builds do not define.
    ss::future<result<raft::replicate_result>> deferred_result() {
        return ss::yield().then([r = next_result()] { return r; });
    }

    result<raft::replicate_result> next_result() const {
        if (fail_with.has_value()) {
            return result<raft::replicate_result>(fail_with.value());
        }
        return result<raft::replicate_result>(raft::replicate_result{
          .last_offset = model::offset(
            commit_at() + std::max(replicates - 1, 0)),
          .last_term = partition_term});
    }
};

offset_commit_request
commit_request(const model::topic_partition& tp, model::offset offset) {
    offset_commit_request r;
    r.data.group_id = test_group;
    offset_commit_request_topic topic;
    topic.name = tp.topic;
    topic.partitions.push_back(
      offset_commit_request_partition{
        .partition_index = tp.partition,
        .committed_offset = offset,
        .committed_leader_epoch = kafka::leader_epoch(1),
        .commit_timestamp = -1,
        .committed_metadata = "",
      });
    r.data.topics.push_back(std::move(topic));
    r.data.retention_time_ms = -1;
    return r;
}

/// Answers the try-abort request with whatever the test set, and records that
/// it was asked.
class deciding_tx_coordinator final : public tx_coordinator_client {
public:
    ss::future<cluster::try_abort_reply> try_abort(
      model::partition_id, model::producer_identity pid, model::tx_seq) final {
        asked_about.push_back(pid);
        cluster::try_abort_reply reply;
        reply.ec = ec;
        reply.commited = cluster::try_abort_reply::committed_type(committed);
        reply.aborted = cluster::try_abort_reply::aborted_type(aborted);
        return ss::make_ready_future<cluster::try_abort_reply>(reply);
    }

    cluster::tx::errc ec{cluster::tx::errc::none};
    bool committed{false};
    bool aborted{true};
    std::vector<model::producer_identity> asked_about;
};

txn_offset_commit_request staged_commit_request(
  model::producer_identity pid,
  const model::topic_partition& tp,
  model::offset offset) {
    txn_offset_commit_request r;
    r.data.group_id = test_group;
    r.data.producer_id = kafka::producer_id(pid.get_id()());
    r.data.producer_epoch = pid.get_epoch();
    txn_offset_commit_request_topic topic;
    topic.name = tp.topic;
    topic.partitions.push_back(
      txn_offset_commit_request_partition{
        .partition_index = tp.partition,
        .committed_offset = offset,
        .committed_leader_epoch = kafka::leader_epoch(1),
        .committed_metadata = std::nullopt,
      });
    r.data.topics.push_back(std::move(topic));
    return r;
}

cluster::commit_group_tx_request
commit_tx_request(model::producer_identity pid, model::tx_seq seq) {
    return cluster::commit_group_tx_request(
      model::ntp{}, pid, seq, test_group, 30s);
}

cluster::abort_group_tx_request
abort_tx_request(model::producer_identity pid, model::tx_seq seq) {
    return cluster::abort_group_tx_request(
      model::ntp{}, test_group, pid, seq, 30s);
}

cluster::begin_group_tx_request
begin_tx_request(model::producer_identity pid, model::tx_seq seq) {
    return cluster::begin_group_tx_request(
      model::ntp{}, test_group, pid, seq, 30s, model::partition_id(0));
}

} // namespace

/// The store is built without a partition, so these cover its state
/// transitions and not its writes.
struct offset_store_test : seastar_test {
    ss::future<> SetUpAsync() override {
        co_await feature_table.start();
        co_await feature_table.invoke_on_all(
          [](features::feature_table& f) { f.testing_activate_all(); });
    }

    ss::future<> TearDownAsync() override {
        co_await store.stop();
        co_await feature_table.stop();
    }

    ss::sharded<features::feature_table> feature_table;

    // owned by the store; the fixture keeps pointers to drive them
    recording_writer* writer = new recording_writer();
    deciding_tx_coordinator* tx_coordinator = new deciding_tx_coordinator();
    bool group_is_dead{false};

    ss::lw_shared_ptr<ss::rwlock> catchup_lock
      = ss::make_lw_shared<ss::rwlock>();

    offset_store store{
      test_group,
      config::shard_local_cfg(),
      catchup_lock,
      std::unique_ptr<offset_writer>(writer),
      model::term_id(1),
      std::unique_ptr<tx_coordinator_client>(tx_coordinator),
      feature_table,
      [this] { return group_is_dead; }};

    /// Fences the producer and hands it the transaction, the way a begin that
    /// reached the log would.
    void open_tx(
      model::producer_identity pid, offset_store::ongoing_transaction tx) {
        store.try_set_fence(pid.get_id(), pid.get_epoch());
        store.insert_ongoing_tx(pid, std::move(tx));
    }

    /// Every write here resolves as soon as it is dispatched, so a test that
    /// only wants the outcome drains both stages at once.
    static ss::future<offset_commit_response>
    drain(offset_commit_stages stages) {
        co_await std::move(stages.dispatched);
        co_return co_await std::move(stages.result);
    }

    ss::future<offset_commit_response> commit(offset_commit_request r) {
        return drain(store.store_offsets(std::move(r)));
    }

    ss::future<offset_commit_response>
    commit(const model::topic_partition& tp, model::offset offset) {
        return commit(commit_request(tp, offset));
    }
};

namespace {

error_code first_partition_error(const offset_commit_response& resp) {
    return resp.data.topics[0].partitions[0].error_code;
}

error_code first_partition_error(const txn_offset_commit_response& resp) {
    return resp.data.topics[0].partitions[0].error_code;
}

/// The expiry interval is read when the store is built, so a test that wants
/// the timer to fire soon needs a store of its own.
struct short_interval_store {
    short_interval_store(
      ss::sharded<features::feature_table>& features,
      ss::lw_shared_ptr<ss::rwlock> lock,
      std::chrono::milliseconds interval) {
        cfg.get("abort_timed_out_transactions_interval_ms").set_value(interval);
        store = std::make_unique<offset_store>(
          test_group,
          config::shard_local_cfg(),
          std::move(lock),
          std::unique_ptr<offset_writer>(writer),
          model::term_id(1),
          std::unique_ptr<tx_coordinator_client>(coordinator),
          features,
          [] { return false; });
    }

    void open_tx(
      model::producer_identity pid, offset_store::ongoing_transaction tx) {
        store->try_set_fence(pid.get_id(), pid.get_epoch());
        store->insert_ongoing_tx(pid, std::move(tx));
    }

    scoped_config cfg;
    recording_writer* writer = new recording_writer();
    deciding_tx_coordinator* coordinator = new deciding_tx_coordinator();
    std::unique_ptr<offset_store> store;
};

} // namespace

TEST_F(offset_store_test, upsert_keeps_the_latest_log_offset) {
    const auto p0 = tp("t", 0);

    ASSERT_TRUE(store.try_upsert_offset(p0, committed(10, 100)));
    ASSERT_EQ(store.offset(p0)->offset, model::offset(100));

    // a commit that landed later in the log wins
    ASSERT_TRUE(store.try_upsert_offset(p0, committed(11, 101)));
    ASSERT_EQ(store.offset(p0)->offset, model::offset(101));

    // the log offset decides, not the offset being committed
    ASSERT_FALSE(store.try_upsert_offset(p0, committed(11, 102)));
    ASSERT_FALSE(store.try_upsert_offset(p0, committed(10, 99)));
    ASSERT_EQ(store.offset(p0)->offset, model::offset(101));
}

TEST_F(offset_store_test, absent_offsets_are_not_reported) {
    ASSERT_FALSE(store.offset(tp("t", 0)).has_value());
    ASSERT_TRUE(store.empty());

    store.try_upsert_offset(tp("t", 0), committed(10, 100));
    ASSERT_FALSE(store.empty());
}

TEST_F(offset_store_test, fence_bump_discards_the_producers_transaction) {
    const model::producer_id pid{7};
    const auto p0 = tp("t", 0);

    open_tx(
      model::producer_identity(pid, model::producer_epoch(1)),
      staged_tx(model::tx_seq(1), p0));
    ASSERT_TRUE(store.has_transactions_in_progress());

    // an equal or older epoch leaves the transaction alone
    store.try_set_fence(pid, model::producer_epoch(1));
    store.try_set_fence(pid, model::producer_epoch(0));
    ASSERT_TRUE(store.has_transactions_in_progress());
    ASSERT_EQ(
      store.producers().find(pid)->second.epoch, model::producer_epoch(1));

    // a newer epoch fences the producer, so its transaction is gone
    store.try_set_fence(pid, model::producer_epoch(2));
    ASSERT_FALSE(store.has_transactions_in_progress());
    ASSERT_EQ(
      store.producers().find(pid)->second.epoch, model::producer_epoch(2));
}

TEST_F(offset_store_test, staged_transactional_offsets_are_unstable) {
    const auto staged = tp("t", 0);
    const auto plain = tp("t", 1);
    store.try_upsert_offset(staged, committed(10, 100));
    store.try_upsert_offset(plain, committed(10, 200));

    ASSERT_FALSE(store.has_pending_transaction(staged));

    open_tx(
      model::producer_identity{7, 1}, staged_tx(model::tx_seq(1), staged));

    // only the topic-partition the transaction staged an offset for
    ASSERT_TRUE(store.has_pending_transaction(staged));
    ASSERT_FALSE(store.has_pending_transaction(plain));
}

TEST_F_CORO(offset_store_test, fetch_reports_unstable_only_when_required) {
    const auto staged = tp("t", 0);
    store.try_upsert_offset(staged, committed(10, 100));
    open_tx(
      model::producer_identity{7, 1}, staged_tx(model::tx_seq(1), staged));

    const auto request = [&staged] {
        offset_fetch_request_group req{.group_id = test_group};
        req.topics.emplace();
        req.topics->push_back(
          offset_fetch_request_topics{
            .name = staged.topic,
            .partition_indexes = {staged.partition, model::partition_id(9)}});
        return req;
    };

    auto unstable = store.fetch_offsets(request(), true);
    ASSERT_EQ_CORO(unstable.topics.size(), 1);
    const auto& unstable_partitions = unstable.topics[0].partitions;
    ASSERT_EQ_CORO(unstable_partitions.size(), 2);
    ASSERT_EQ_CORO(
      unstable_partitions[0].error_code, error_code::unstable_offset_commit);
    ASSERT_EQ_CORO(unstable_partitions[0].committed_offset, model::offset(-1));

    auto stable = store.fetch_offsets(request(), false);
    const auto& stable_partitions = stable.topics[0].partitions;
    ASSERT_EQ_CORO(stable_partitions[0].error_code, error_code::none);
    ASSERT_EQ_CORO(stable_partitions[0].committed_offset, model::offset(100));

    // a topic-partition the group has no offset for reads as -1, not an error
    ASSERT_EQ_CORO(stable_partitions[1].error_code, error_code::none);
    ASSERT_EQ_CORO(stable_partitions[1].committed_offset, model::offset(-1));
}

TEST_F_CORO(offset_store_test, fetch_with_no_topics_reads_every_offset) {
    store.try_upsert_offset(tp("t1", 0), committed(10, 100));
    store.try_upsert_offset(tp("t2", 3), committed(11, 200));

    auto resp = store.fetch_offsets(
      offset_fetch_request_group{.group_id = test_group}, false);

    const auto committed_offset =
      [&resp](const model::topic_partition& want) -> model::offset {
        for (const auto& t : resp.topics) {
            for (const auto& p : t.partitions) {
                if (
                  t.name == want.topic && p.partition_index == want.partition) {
                    return p.committed_offset;
                }
            }
        }
        return model::offset{};
    };

    ASSERT_EQ_CORO(resp.topics.size(), 2);
    ASSERT_EQ_CORO(committed_offset(tp("t1", 0)), model::offset(100));
    ASSERT_EQ_CORO(committed_offset(tp("t2", 3)), model::offset(200));
}

TEST_F(offset_store_test, term_change_drops_transaction_state) {
    store.try_upsert_offset(tp("t", 0), committed(10, 100));
    open_tx(
      model::producer_identity{7, 1}, staged_tx(model::tx_seq(1), tp("t", 0)));

    store.reset_tx_state(model::term_id(2));

    // the committed offset survives; only transaction state is dropped
    ASSERT_EQ(store.term(), model::term_id(2));
    ASSERT_TRUE(store.producers().empty());
    ASSERT_FALSE(store.has_transactions_in_progress());
    ASSERT_EQ(store.offset(tp("t", 0))->offset, model::offset(100));
}

TEST_F(offset_store_test, removing_offsets_returns_what_was_dropped) {
    store.try_upsert_offset(tp("t", 0), committed(10, 100));
    store.try_upsert_offset(tp("t", 1), committed(10, 200));
    store.try_upsert_offset(tp("other", 0), committed(10, 300));

    auto removed = store.remove_offsets(
      chunked_vector<model::topic_partition>::single(tp("t", 0)));

    ASSERT_EQ(removed.size(), 1);
    ASSERT_EQ(removed[0].first, tp("t", 0));
    ASSERT_EQ(removed[0].second.offset, model::offset(100));
    ASSERT_FALSE(store.offset(tp("t", 0)).has_value());
    ASSERT_EQ(store.offsets().size(), 2);

    // a topic-partition the group has no offset for is not reported as removed
    ASSERT_TRUE(store
                  .remove_offsets(
                    chunked_vector<model::topic_partition>::single(tp("t", 0)))
                  .empty());
}

TEST_F(offset_store_test, removing_a_topics_last_partition_drops_the_topic) {
    store.try_upsert_offset(tp("t", 0), committed(10, 100));
    store.try_upsert_offset(tp("t", 1), committed(10, 200));
    store.try_upsert_offset(tp("other", 0), committed(10, 300));

    chunked_vector<model::topic_partition> both;
    both.push_back(tp("t", 0));
    both.push_back(tp("t", 1));

    ASSERT_EQ(store.remove_offsets(both).size(), 2);

    // the topic goes with its last partition; no empty partition map is kept
    ASSERT_FALSE(store.offsets().contains(model::topic("t")));
    ASSERT_EQ(store.offsets().size(), 1);
}

TEST_F(offset_store_test, expiry_skips_subscribed_and_non_reclaimable) {
    constexpr auto retention = 24h;
    const auto long_ago = model::timestamp(
      model::timestamp::now().value()
      - std::chrono::milliseconds(retention * 2).count());

    store.try_upsert_offset(tp("expired", 0), committed(10, 100, long_ago));
    store.try_upsert_offset(tp("fresh", 0), committed(10, 100));

    auto legacy = committed(10, 100, long_ago);
    legacy.non_reclaimable = true;
    store.try_upsert_offset(tp("legacy", 0), legacy);

    auto expired = store.filter_expired_offsets(
      std::chrono::duration_cast<std::chrono::seconds>(retention),
      none_subscribed,
      expires_at_commit);

    // only the offset whose retention has passed and which nothing retains
    ASSERT_EQ(expired.size(), 1);
    ASSERT_EQ(expired[0], tp("expired", 0));

    // a subscribed topic's offset is retained however old it is
    ASSERT_TRUE(store
                  .filter_expired_offsets(
                    std::chrono::duration_cast<std::chrono::seconds>(retention),
                    all_subscribed,
                    expires_at_commit)
                  .empty());
}

TEST_F(offset_store_test, expiry_honours_a_per_offset_expiry_timestamp) {
    constexpr auto retention = 24h;
    const auto retention_secs
      = std::chrono::duration_cast<std::chrono::seconds>(retention);
    const auto now = model::timestamp::now();

    // retention has not passed, but the offset's own expiry timestamp has
    auto explicitly_expired = committed(10, 100);
    explicitly_expired.expiry_timestamp = model::timestamp(now.value() - 1);
    store.try_upsert_offset(tp("explicit", 0), explicitly_expired);

    auto not_yet = committed(10, 100);
    not_yet.expiry_timestamp = model::timestamp(now.value() + 3600'000);
    store.try_upsert_offset(tp("later", 0), not_yet);

    auto expired = store.filter_expired_offsets(
      retention_secs, none_subscribed, expires_at_commit);

    ASSERT_EQ(expired.size(), 1);
    ASSERT_EQ(expired[0], tp("explicit", 0));
}

TEST_F(offset_store_test, tombstone_record_carries_no_value) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    store.add_offset_tombstone_record(tp("t", 3), builder);
    auto batch = std::move(builder).build();

    ASSERT_EQ(batch.record_count(), 1);
    auto records = batch.copy_records();
    auto kv = group_metadata_serializer::decode_offset_metadata(
      std::move(*records.begin()));

    ASSERT_EQ(kv.key.group_id, test_group);
    ASSERT_EQ(kv.key.topic, model::topic("t"));
    ASSERT_EQ(kv.key.partition, model::partition_id(3));
    ASSERT_FALSE(kv.value.has_value());
}

TEST_F(offset_store_test, commit_record_carries_the_offset) {
    const auto commit_ts = model::timestamp(1234);
    const auto expiry_ts = model::timestamp(5678);

    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    store.update_store_offset_builder(
      builder,
      model::topic("t"),
      model::partition_id(3),
      model::offset(100),
      kafka::leader_epoch(2),
      "note",
      commit_ts,
      expiry_ts);
    auto batch = std::move(builder).build();

    ASSERT_EQ(batch.record_count(), 1);
    auto records = batch.copy_records();
    auto kv = group_metadata_serializer::decode_offset_metadata(
      std::move(*records.begin()));

    ASSERT_EQ(kv.key.group_id, test_group);
    ASSERT_EQ(kv.key.topic, model::topic("t"));
    ASSERT_EQ(kv.key.partition, model::partition_id(3));
    ASSERT_TRUE(kv.value.has_value());
    ASSERT_EQ(kv.value->offset, model::offset(100));
    ASSERT_EQ(kv.value->metadata, "note");
    ASSERT_EQ(kv.value->commit_timestamp, commit_ts);
    ASSERT_EQ(kv.value->expiry_timestamp, expiry_ts);

    // an expiry timestamp pins the record at value version 1, which has no
    // field for the leader epoch
    ASSERT_EQ(kv.value->leader_epoch, kafka::leader_epoch(-1));
}

TEST_F(offset_store_test, commit_record_keeps_the_leader_epoch_without_expiry) {
    cluster::simple_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    store.update_store_offset_builder(
      builder,
      model::topic("t"),
      model::partition_id(0),
      model::offset(100),
      kafka::leader_epoch(1),
      "",
      model::timestamp(1234),
      std::nullopt);
    auto batch = std::move(builder).build();

    auto records = batch.copy_records();
    auto kv = group_metadata_serializer::decode_offset_metadata(
      std::move(*records.begin()));

    // without an expiry the record is written at the latest value version,
    // which does carry the leader epoch
    ASSERT_EQ(kv.value->leader_epoch, kafka::leader_epoch(1));
    ASSERT_EQ(kv.value->expiry_timestamp, model::timestamp(-1));
}

TEST_F(offset_store_test, removing_one_offset_leaves_the_others) {
    store.try_upsert_offset(tp("t", 0), committed(10, 100));
    store.try_upsert_offset(tp("t", 1), committed(10, 200));

    store.erase_offset(tp("t", 0));

    ASSERT_FALSE(store.offset(tp("t", 0)).has_value());
    ASSERT_EQ(store.offset(tp("t", 1))->offset, model::offset(200));
}

TEST_F(offset_store_test, a_transaction_expires_at_its_deadline) {
    auto tx = live_tx(model::tx_seq(1));
    ASSERT_FALSE(tx.is_expired());
    ASSERT_GT(tx.deadline(), model::timeout_clock::now());

    // requesting expiration expires it regardless of the deadline
    tx.is_expiration_requested = true;
    ASSERT_TRUE(tx.is_expired());

    offset_store::ongoing_transaction elapsed(
      model::tx_seq(1), model::partition_id(0), 0ms, model::offset(100));
    ASSERT_TRUE(elapsed.is_expired());

    elapsed.timeout = 30s;
    elapsed.update_last_update_time();
    ASSERT_FALSE(elapsed.is_expired());
}

TEST_F_CORO(offset_store_test, the_producer_lock_serializes_by_producer) {
    int running = 0;
    int most_concurrent = 0;
    ss::promise<> release_first;

    const auto operation = [&running, &most_concurrent](auto f) {
        return [&running, &most_concurrent, f = std::move(f)]() mutable {
            ++running;
            most_concurrent = std::max(most_concurrent, running);
            return f().then([&running] {
                --running;
                return 0;
            });
        };
    };

    auto held = store.with_pid_lock(
      model::producer_id(7),
      operation([&release_first] { return release_first.get_future(); }));

    // a second operation for the same producer waits for the first
    auto queued = store.with_pid_lock(
      model::producer_id(7), operation([] { return ss::now(); }));

    // a different producer is not blocked by it
    co_await store.with_pid_lock(
      model::producer_id(8), operation([] { return ss::now(); }));

    ASSERT_EQ_CORO(most_concurrent, 2);

    // producer 7 still holds the lock, so the operation behind it has not run
    ASSERT_FALSE_CORO(queued.available());

    release_first.set_value();
    co_await std::move(held);
    co_await std::move(queued);

    ASSERT_EQ_CORO(running, 0);
}

TEST_F_CORO(offset_store_test, a_committed_offset_becomes_readable) {
    const auto p0 = tp("t", 0);

    auto resp = co_await commit(p0, model::offset(100));

    ASSERT_EQ_CORO(resp.data.topics.size(), 1);
    ASSERT_EQ_CORO(first_partition_error(resp), error_code::none);
    ASSERT_EQ_CORO(writer->written.size(), 1);
    ASSERT_EQ_CORO(writer->terms[0], model::term_id(1));

    ASSERT_EQ_CORO(store.offset(p0)->offset, model::offset(100));
    // the commit's log offset is the one raft reported
    ASSERT_EQ_CORO(store.offset(p0)->log_offset, writer->commit_at);
    ASSERT_FALSE_CORO(store.has_pending_transaction(p0));
}

TEST_F_CORO(offset_store_test, a_failed_commit_stores_nothing) {
    const auto p0 = tp("t", 0);
    writer->fail_with = raft::errc::timeout;

    auto resp = co_await commit(p0, model::offset(100));

    ASSERT_EQ_CORO(first_partition_error(resp), error_code::request_timed_out);
    ASSERT_FALSE_CORO(store.offset(p0).has_value());

    // the commit is no longer in flight, so a fetch is stable again
    ASSERT_FALSE_CORO(store.has_pending_transaction(p0));
    ASSERT_TRUE_CORO(store.empty());
}

TEST_F_CORO(offset_store_test, a_commit_that_lands_after_the_group_dies) {
    const auto p0 = tp("t", 0);
    group_is_dead = true;

    auto resp = co_await commit(p0, model::offset(100));

    // the record is written, but a dying group's state is not touched
    ASSERT_EQ_CORO(first_partition_error(resp), error_code::none);
    ASSERT_EQ_CORO(writer->written.size(), 1);
    ASSERT_FALSE_CORO(store.offset(p0).has_value());
}

TEST_F_CORO(offset_store_test, a_commit_with_no_partitions_writes_nothing) {
    offset_commit_request empty;
    empty.data.group_id = test_group;
    empty.data.retention_time_ms = -1;

    co_await commit(std::move(empty));

    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_test, a_write_in_an_older_term_is_refused) {
    writer->partition_term = model::term_id(2);

    auto reply = co_await store.begin_tx(
      begin_tx_request(model::producer_identity{7, 0}, model::tx_seq(1)));

    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::stale);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_test, a_failed_transaction_write_sheds_leadership) {
    writer->fail_with = raft::errc::timeout;

    auto reply = co_await store.begin_tx(
      begin_tx_request(model::producer_identity{7, 0}, model::tx_seq(1)));

    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::timeout);
    ASSERT_EQ_CORO(writer->step_downs, 1);
    ASSERT_FALSE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(offset_store_test, a_transaction_commits_its_staged_offsets) {
    const auto pid = model::producer_identity{7, 0};
    const auto p0 = tp("t", 0);

    auto begun = co_await store.begin_tx(
      begin_tx_request(pid, model::tx_seq(1)));
    ASSERT_EQ_CORO(begun.ec, cluster::tx::errc::none);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());

    auto staged = co_await store.store_txn_offsets(
      staged_commit_request(pid, p0, model::offset(100)));
    ASSERT_FALSE_CORO(staged.data.errored());

    // staged, so a fetch cannot read it yet
    ASSERT_FALSE_CORO(store.offset(p0).has_value());
    ASSERT_TRUE_CORO(store.has_pending_transaction(p0));

    auto committed = co_await store.commit_tx(
      commit_tx_request(pid, model::tx_seq(1)));
    ASSERT_EQ_CORO(committed.ec, cluster::tx::errc::none);

    ASSERT_EQ_CORO(store.offset(p0)->offset, model::offset(100));
    ASSERT_FALSE_CORO(store.has_transactions_in_progress());
    ASSERT_FALSE_CORO(store.has_pending_transaction(p0));
}

TEST_F_CORO(offset_store_test, an_aborted_transaction_discards_its_offsets) {
    const auto pid = model::producer_identity{7, 0};
    const auto p0 = tp("t", 0);

    co_await store.begin_tx(begin_tx_request(pid, model::tx_seq(1)));
    co_await store.store_txn_offsets(
      staged_commit_request(pid, p0, model::offset(100)));

    auto aborted = co_await store.abort_tx(
      abort_tx_request(pid, model::tx_seq(1)));
    ASSERT_EQ_CORO(aborted.ec, cluster::tx::errc::none);

    ASSERT_FALSE_CORO(store.offset(p0).has_value());
    ASSERT_FALSE_CORO(store.has_transactions_in_progress());
    ASSERT_FALSE_CORO(store.has_pending_transaction(p0));
}

TEST_F_CORO(offset_store_test, staging_without_a_fence_is_refused) {
    auto staged = co_await store.store_txn_offsets(staged_commit_request(
      model::producer_identity{7, 0}, tp("t", 0), model::offset(100)));

    ASSERT_EQ_CORO(
      first_partition_error(staged), error_code::invalid_producer_epoch);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_test, a_fenced_producer_cannot_begin) {
    const model::producer_id id{7};
    store.try_set_fence(id, model::producer_epoch(2));

    auto reply = co_await store.begin_tx(
      begin_tx_request(model::producer_identity{id(), 1}, model::tx_seq(1)));

    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::fenced);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_test, an_expired_transaction_is_aborted) {
    const auto pid = model::producer_identity{7, 0};
    open_tx(pid, expired_tx(model::tx_seq(1)));
    tx_coordinator->aborted = true;

    auto ec = co_await store.abort_txes(true);

    ASSERT_EQ_CORO(ec, cluster::tx::errc::none);
    ASSERT_EQ_CORO(tx_coordinator->asked_about.size(), 1);
    ASSERT_EQ_CORO(tx_coordinator->asked_about[0], pid);
    ASSERT_FALSE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(
  offset_store_test, an_expired_transaction_the_coordinator_committed) {
    const auto pid = model::producer_identity{7, 0};
    const auto p0 = tp("t", 0);
    store.try_set_fence(pid.get_id(), pid.get_epoch());

    auto tx = expired_tx(model::tx_seq(1));
    tx.offsets[p0] = staged_offset(p0, model::offset(100));
    store.insert_ongoing_tx(pid, std::move(tx));

    tx_coordinator->committed = true;
    tx_coordinator->aborted = false;

    auto ec = co_await store.abort_txes(true);

    // the coordinator settled it as committed, so the staged offset lands
    ASSERT_EQ_CORO(ec, cluster::tx::errc::none);
    ASSERT_EQ_CORO(store.offset(p0)->offset, model::offset(100));
    ASSERT_FALSE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(offset_store_test, an_unsettled_transaction_is_left_alone) {
    const auto pid = model::producer_identity{7, 0};
    open_tx(pid, expired_tx(model::tx_seq(1)));
    tx_coordinator->committed = false;
    tx_coordinator->aborted = false;

    auto ec = co_await store.abort_txes(true);

    ASSERT_EQ_CORO(ec, cluster::tx::errc::stale);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(offset_store_test, a_live_transaction_expires_only_when_asked) {
    const auto pid = model::producer_identity{7, 0};
    open_tx(pid, live_tx(model::tx_seq(1)));

    co_await store.abort_txes(true);
    ASSERT_TRUE_CORO(tx_coordinator->asked_about.empty());
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());

    co_await store.abort_txes(false);
    ASSERT_EQ_CORO(tx_coordinator->asked_about.size(), 1);
    ASSERT_FALSE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(offset_store_test, no_transaction_is_touched_while_loading) {
    const auto pid = model::producer_identity{7, 0};
    open_tx(pid, expired_tx(model::tx_seq(1)));

    // recovery holds the catchup lock for writing
    auto held = co_await catchup_lock->hold_write_lock();

    auto ec = co_await store.abort_txes(true);

    ASSERT_EQ_CORO(ec, cluster::tx::errc::stale);
    ASSERT_TRUE_CORO(tx_coordinator->asked_about.empty());
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(offset_store_test, the_timer_expires_a_transaction_on_its_own) {
    short_interval_store timed(feature_table, catchup_lock, 20ms);
    timed.open_tx(model::producer_identity{7, 0}, expired_tx(model::tx_seq(1)));

    // nothing else drives expiration, so the timer is what clears it
    const auto deadline = ss::lowres_clock::now() + 30s;
    while (timed.store->has_transactions_in_progress()
           && ss::lowres_clock::now() < deadline) {
        co_await ss::sleep(10ms);
    }

    // stopped before asserting, so a failure cannot leave the sweep it spawned
    // outstanding on a gate that is about to be destroyed
    co_await timed.store->stop();

    ASSERT_FALSE_CORO(timed.store->has_transactions_in_progress());
    ASSERT_EQ_CORO(timed.coordinator->asked_about.size(), 1);
}

/// Opens a transaction the way `begin_tx` does, so the rejection cases below
/// start from a producer with one in flight.
struct offset_store_tx_test : offset_store_test {
    static constexpr auto pid = model::producer_identity{7, 1};
    static constexpr auto seq = model::tx_seq(1);

    ss::future<> open_transaction() {
        open_tx(pid, live_tx(seq));
        return ss::now();
    }
};

enum class settlement { commit, abort };

/// The commit and abort paths apply the same guards before they write their
/// marker, so each case here runs through both.
struct offset_store_settle_test
  : offset_store_tx_test
  , testing::WithParamInterface<settlement> {
    /// Settles the transaction whichever way the parameter asks for, and
    /// reports the error code, which is what the two replies have in common.
    ss::future<cluster::tx::errc>
    settle(model::producer_identity producer, model::tx_seq sequence) {
        if (GetParam() == settlement::commit) {
            auto reply = co_await store.commit_tx(
              commit_tx_request(producer, sequence));
            co_return reply.ec;
        }
        auto reply = co_await store.abort_tx(
          abort_tx_request(producer, sequence));
        co_return reply.ec;
    }

    ss::future<cluster::tx::errc> settle() { return settle(pid, seq); }
};

INSTANTIATE_TEST_SUITE_P(
  either_way,
  offset_store_settle_test,
  testing::Values(settlement::commit, settlement::abort),
  [](const testing::TestParamInfo<settlement>& info) {
      return info.param == settlement::commit ? "commit" : "abort";
  });

TEST_P_CORO(offset_store_settle_test, settling_in_an_older_term_is_stale) {
    co_await open_transaction();
    writer->partition_term = model::term_id(2);

    const auto ec = co_await settle();

    ASSERT_EQ_CORO(ec, cluster::tx::errc::stale);
    ASSERT_TRUE_CORO(writer->written.empty());
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_P_CORO(
  offset_store_settle_test, settling_a_forgotten_transaction_succeeds) {
    // the coordinator is replaying a settlement whose state we no longer hold,
    // so it must already have been applied
    const auto ec = co_await settle();

    ASSERT_EQ_CORO(ec, cluster::tx::errc::none);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_P_CORO(offset_store_settle_test, settling_at_a_fenced_epoch_is_rejected) {
    co_await open_transaction();

    const auto ec = co_await settle(
      model::producer_identity{pid.get_id()(), 2}, seq);

    ASSERT_EQ_CORO(ec, cluster::tx::errc::request_rejected);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_P_CORO(offset_store_settle_test, settling_a_superseded_sequence_succeeds) {
    co_await open_transaction();

    // a later transaction is already open, so this one must have been settled
    const auto ec = co_await settle(pid, model::tx_seq(seq() - 1));

    ASSERT_EQ_CORO(ec, cluster::tx::errc::none);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_P_CORO(offset_store_settle_test, settling_a_later_sequence_is_rejected) {
    co_await open_transaction();

    const auto ec = co_await settle(pid, model::tx_seq(seq() + 1));

    ASSERT_EQ_CORO(ec, cluster::tx::errc::request_rejected);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_P_CORO(
  offset_store_settle_test, a_failed_settlement_write_sheds_leadership) {
    co_await open_transaction();
    writer->fail_with = raft::errc::timeout;

    const auto ec = co_await settle();

    ASSERT_EQ_CORO(ec, cluster::tx::errc::timeout);
    ASSERT_EQ_CORO(writer->step_downs, 1);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_P_CORO(
  offset_store_settle_test,
  settling_for_a_producer_with_no_transaction_succeeds) {
    // fenced, but nothing was begun, so the settlement already happened
    store.try_set_fence(pid.get_id(), pid.get_epoch());

    const auto ec = co_await settle();

    ASSERT_EQ_CORO(ec, cluster::tx::errc::none);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_tx_test, staging_in_an_older_term_is_refused) {
    co_await open_transaction();
    writer->partition_term = model::term_id(2);

    auto staged = co_await store.store_txn_offsets(
      staged_commit_request(pid, tp("t", 0), model::offset(100)));

    ASSERT_EQ_CORO(
      first_partition_error(staged), error_code::unknown_server_error);
}

TEST_F_CORO(offset_store_tx_test, staging_at_a_fenced_epoch_is_refused) {
    co_await open_transaction();

    auto staged = co_await store.store_txn_offsets(staged_commit_request(
      model::producer_identity{pid.get_id()(), 2},
      tp("t", 0),
      model::offset(100)));

    ASSERT_EQ_CORO(
      first_partition_error(staged), error_code::invalid_producer_epoch);
}

TEST_F_CORO(
  offset_store_tx_test, staging_without_an_open_transaction_is_refused) {
    // fenced, but no transaction was begun
    store.try_set_fence(pid.get_id(), pid.get_epoch());

    auto staged = co_await store.store_txn_offsets(
      staged_commit_request(pid, tp("t", 0), model::offset(100)));

    ASSERT_EQ_CORO(
      first_partition_error(staged), error_code::invalid_producer_epoch);
}

TEST_F_CORO(offset_store_tx_test, beginning_again_is_idempotent) {
    co_await open_transaction();

    auto reply = co_await store.begin_tx(begin_tx_request(pid, seq));

    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::none);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_tx_test, beginning_with_another_sequence_is_refused) {
    co_await open_transaction();

    auto reply = co_await store.begin_tx(
      begin_tx_request(pid, model::tx_seq(seq() + 1)));

    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::unknown_server_error);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_tx_test, beginning_again_after_staging_is_refused) {
    co_await open_transaction();
    co_await store.store_txn_offsets(
      staged_commit_request(pid, tp("t", 0), model::offset(100)));
    writer->written.clear();

    auto reply = co_await store.begin_tx(begin_tx_request(pid, seq));

    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::unknown_server_error);
    ASSERT_TRUE_CORO(writer->written.empty());
}

TEST_F_CORO(offset_store_tx_test, a_newer_epoch_aborts_the_old_transaction) {
    co_await open_transaction();
    const auto bumped = model::producer_identity{pid.get_id()(), 2};
    tx_coordinator->aborted = true;

    auto reply = co_await store.begin_tx(begin_tx_request(bumped, seq));

    // the old transaction is settled with the coordinator before the new one
    // opens, rather than waiting for it to time out
    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::none);
    ASSERT_EQ_CORO(tx_coordinator->asked_about.size(), 1);
    ASSERT_EQ_CORO(tx_coordinator->asked_about[0], pid);
    ASSERT_EQ_CORO(
      store.producers().find(pid.get_id())->second.epoch,
      model::producer_epoch(2));
}

TEST_F_CORO(offset_store_tx_test, a_newer_epoch_waits_on_an_unsettled_old_one) {
    co_await open_transaction();
    tx_coordinator->committed = false;
    tx_coordinator->aborted = false;

    auto reply = co_await store.begin_tx(
      begin_tx_request(model::producer_identity{pid.get_id()(), 2}, seq));

    ASSERT_EQ_CORO(reply.ec, cluster::tx::errc::stale);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_F(offset_store_test, a_later_commit_of_a_lower_offset_still_wins) {
    const auto p0 = tp("t", 0);
    store.try_upsert_offset(p0, committed(10, 100));

    // a client that rewound: later in the log, lower offset
    ASSERT_TRUE(store.try_upsert_offset(p0, committed(11, 50)));
    ASSERT_EQ(store.offset(p0)->offset, model::offset(50));
}

/// A replication failure and the transaction error the client is told.
struct replication_error_case {
    std::string_view name;
    std::error_code replication_error;
    cluster::tx::errc expected;
};

struct offset_store_error_mapping_test
  : offset_store_tx_test
  , testing::WithParamInterface<replication_error_case> {};

INSTANTIATE_TEST_SUITE_P(
  categories,
  offset_store_error_mapping_test,
  testing::Values(
    replication_error_case{
      "cluster_timeout",
      make_error_code(cluster::errc::timeout),
      cluster::tx::errc::timeout},
    replication_error_case{
      "cluster_replication_error",
      make_error_code(cluster::errc::replication_error),
      cluster::tx::errc::not_coordinator},
    replication_error_case{
      "no_known_category",
      std::make_error_code(std::errc::io_error),
      cluster::tx::errc::not_coordinator}),
  [](const testing::TestParamInfo<replication_error_case>& info) {
      return std::string(info.param.name);
  });

TEST_P_CORO(offset_store_error_mapping_test, the_failure_reaches_the_client) {
    co_await open_transaction();
    writer->fail_with = GetParam().replication_error;

    auto reply = co_await store.commit_tx(commit_tx_request(pid, seq));

    ASSERT_EQ_CORO(reply.ec, GetParam().expected);
    ASSERT_EQ_CORO(writer->step_downs, 1);
}

TEST_F_CORO(offset_store_tx_test, a_failed_staging_write_sheds_leadership) {
    co_await open_transaction();
    writer->fail_with = make_error_code(raft::errc::timeout);

    auto staged = co_await store.store_txn_offsets(
      staged_commit_request(pid, tp("t", 0), model::offset(100)));

    ASSERT_EQ_CORO(
      first_partition_error(staged), error_code::request_timed_out);
    ASSERT_EQ_CORO(writer->step_downs, 1);
    ASSERT_FALSE_CORO(store.has_pending_transaction(tp("t", 0)));
}

TEST_F_CORO(offset_store_test, a_failed_expiry_commit_leaves_the_transaction) {
    const auto pid = model::producer_identity{7, 0};
    open_tx(pid, expired_tx(model::tx_seq(1)));

    // the coordinator settles it as committed, but writing the marker fails
    tx_coordinator->committed = true;
    tx_coordinator->aborted = false;
    writer->fail_with = make_error_code(raft::errc::timeout);

    auto ec = co_await store.abort_txes(true);

    ASSERT_EQ_CORO(ec, cluster::tx::errc::timeout);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(offset_store_test, a_failed_expiry_abort_leaves_the_transaction) {
    const auto pid = model::producer_identity{7, 0};
    open_tx(pid, expired_tx(model::tx_seq(1)));

    tx_coordinator->aborted = true;
    writer->fail_with = make_error_code(raft::errc::timeout);

    auto ec = co_await store.abort_txes(true);

    ASSERT_EQ_CORO(ec, cluster::tx::errc::timeout);
    ASSERT_TRUE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(
  offset_store_test, a_commit_with_a_retention_time_stores_an_expiry) {
    const auto p0 = tp("t", 0);
    auto r = commit_request(p0, model::offset(100));
    r.data.retention_time_ms = 60'000;

    co_await commit(std::move(r));

    ASSERT_TRUE_CORO(store.offset(p0)->expiry_timestamp.has_value());
}

TEST_F_CORO(offset_store_test, a_commit_can_carry_its_own_timestamp) {
    const auto p0 = tp("t", 0);
    auto r = commit_request(p0, model::offset(100));
    r.data.topics[0].partitions[0].commit_timestamp = 12'345;

    co_await commit(std::move(r));

    ASSERT_EQ_CORO(
      store.offset(p0)->commit_timestamp, model::timestamp(12'345));
}

TEST_F_CORO(offset_store_test, two_commits_in_flight_for_one_partition) {
    const auto p0 = tp("t", 0);

    // both are recorded as in flight before either completes
    auto first = store.store_offsets(commit_request(p0, model::offset(100)));
    auto second = store.store_offsets(commit_request(p0, model::offset(200)));

    co_await drain(std::move(first));
    co_await drain(std::move(second));

    ASSERT_EQ_CORO(store.offset(p0)->offset, model::offset(200));
    ASSERT_FALSE_CORO(store.has_pending_transaction(p0));
}

TEST_F_CORO(
  offset_store_test, an_offset_with_a_commit_in_flight_is_not_expired) {
    constexpr auto retention = 24h;
    const auto p0 = tp("t", 0);
    const auto long_ago = model::timestamp(
      model::timestamp::now().value()
      - std::chrono::milliseconds(retention * 2).count());
    store.try_upsert_offset(p0, committed(10, 100, long_ago));

    // in flight, and not yet applied
    auto in_flight = store.store_offsets(
      commit_request(p0, model::offset(200)));

    auto expired = store.filter_expired_offsets(
      std::chrono::duration_cast<std::chrono::seconds>(retention),
      none_subscribed,
      expires_at_commit);
    ASSERT_TRUE_CORO(expired.empty());

    co_await drain(std::move(in_flight));
}

TEST_F_CORO(offset_store_test, fetching_every_offset_reports_the_unstable_one) {
    const auto staged = tp("t", 0);
    const auto plain = tp("t", 1);
    store.try_upsert_offset(staged, committed(10, 100));
    store.try_upsert_offset(plain, committed(10, 200));
    open_tx(
      model::producer_identity{7, 1}, staged_tx(model::tx_seq(1), staged));

    auto resp = store.fetch_offsets(
      offset_fetch_request_group{.group_id = test_group}, true);

    ASSERT_EQ_CORO(resp.topics.size(), 1);
    for (const auto& p : resp.topics[0].partitions) {
        if (p.partition_index == staged.partition) {
            ASSERT_EQ_CORO(p.error_code, error_code::unstable_offset_commit);
        } else {
            ASSERT_EQ_CORO(p.committed_offset, model::offset(200));
        }
    }
}

TEST_F_CORO(
  offset_store_test, two_failing_commits_in_flight_for_one_partition) {
    const auto p0 = tp("t", 0);
    writer->fail_with = make_error_code(raft::errc::timeout);

    auto first = store.store_offsets(commit_request(p0, model::offset(100)));
    auto second = store.store_offsets(commit_request(p0, model::offset(200)));

    co_await drain(std::move(first));
    co_await drain(std::move(second));

    // only the failure carrying the pending offset clears it, so the pending
    // commit does not survive the pair
    ASSERT_FALSE_CORO(store.offset(p0).has_value());
    ASSERT_FALSE_CORO(store.has_pending_transaction(p0));
}

TEST_F_CORO(
  offset_store_test, a_commit_that_lands_after_its_offset_is_dropped_is_lost) {
    const auto p0 = tp("t", 0);
    auto in_flight = store.store_offsets(
      commit_request(p0, model::offset(100)));

    // the offset goes away while the write is in flight
    store.remove_pending_offset_commit(p0);

    co_await drain(std::move(in_flight));

    ASSERT_FALSE_CORO(store.offset(p0).has_value());
}

TEST_F_CORO(
  offset_store_test, a_failed_commit_whose_offset_was_dropped_is_ignored) {
    const auto p0 = tp("t", 0);
    writer->fail_with = make_error_code(raft::errc::timeout);
    auto in_flight = store.store_offsets(
      commit_request(p0, model::offset(100)));

    store.remove_pending_offset_commit(p0);

    co_await drain(std::move(in_flight));

    ASSERT_FALSE_CORO(store.offset(p0).has_value());
    ASSERT_TRUE_CORO(store.empty());
}

TEST_F_CORO(
  offset_store_test, a_stable_fetch_reports_an_in_flight_commit_as_unstable) {
    const auto p0 = tp("t", 0);
    store.try_upsert_offset(p0, committed(10, 100));

    auto in_flight = store.store_offsets(
      commit_request(p0, model::offset(200)));

    auto resp = store.fetch_offsets(
      offset_fetch_request_group{.group_id = test_group}, true);
    ASSERT_EQ_CORO(resp.topics.size(), 1);
    ASSERT_EQ_CORO(
      resp.topics[0].partitions[0].error_code,
      error_code::unstable_offset_commit);

    co_await drain(std::move(in_flight));
}

TEST_F_CORO(offset_store_test, the_sweep_skips_a_producer_with_no_transaction) {
    const auto fenced = model::producer_identity{7, 0};
    const auto with_tx = model::producer_identity{8, 0};
    store.try_set_fence(fenced.get_id(), fenced.get_epoch());
    open_tx(with_tx, expired_tx(model::tx_seq(1)));

    co_await store.abort_txes(true);

    ASSERT_EQ_CORO(tx_coordinator->asked_about.size(), 1);
    ASSERT_EQ_CORO(tx_coordinator->asked_about[0], with_tx);
    ASSERT_FALSE_CORO(store.has_transactions_in_progress());
}

TEST_F_CORO(offset_store_test, the_timer_leaves_live_transactions_alone) {
    short_interval_store timed(feature_table, catchup_lock, 20ms);
    for (auto id : {7, 8}) {
        timed.open_tx(
          model::producer_identity{id, 0}, live_tx(model::tx_seq(1)));
    }

    // long enough for several sweeps, none of which has anything to expire
    co_await ss::sleep(100ms);

    co_await timed.store->stop();

    ASSERT_TRUE_CORO(timed.store->has_transactions_in_progress());
    ASSERT_TRUE_CORO(timed.coordinator->asked_about.empty());
}

// an open transaction stages its offsets on the producer, so the guard reads
// there as well as in the pending commit map
TEST_F(offset_store_test, expiry_skips_an_offset_a_transaction_staged) {
    constexpr auto retention = 24h;
    const auto retention_secs
      = std::chrono::duration_cast<std::chrono::seconds>(retention);
    const auto long_ago = model::timestamp(
      model::timestamp::now().value()
      - std::chrono::milliseconds(retention * 2).count());

    const auto staged = tp("t", 0);
    const auto idle = tp("t", 1);
    store.try_upsert_offset(staged, committed(10, 100, long_ago));
    store.try_upsert_offset(idle, committed(10, 100, long_ago));

    open_tx(
      model::producer_identity{7, 1}, staged_tx(model::tx_seq(1), staged));

    auto expired = store.filter_expired_offsets(
      retention_secs, none_subscribed, expires_at_commit);

    ASSERT_EQ(expired.size(), 1);
    ASSERT_EQ(expired[0], idle);
}
