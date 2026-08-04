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
#include "absl/container/flat_hash_map.h"
#include "base/format_to.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tx_protocol_types.h"
#include "cluster/tx_utils.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "features/feature_table.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/server/group_probe.h"
#include "kafka/server/offset_writer.h"
#include "kafka/server/stages.h"
#include "kafka/server/tx_coordinator_client.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "ssx/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <functional>
#include <memory>
#include <optional>

namespace kafka {

kafka::error_code map_store_offset_error_code(std::error_code);

/// \brief The committed and transactional offset state of one consumer group.
///
/// Owns the group's committed offsets, the commits in flight to its
/// `__consumer_offsets` partition, and the per-producer transaction state that
/// stages offsets until a transaction commits, and writes all three to that
/// partition. Committed offsets are last-write-wins by log offset, and a record
/// is applied to state only once it commits.
///
/// The owning group validates a request before calling in: whether a commit is
/// allowed for the requesting member, and which offsets are candidates for
/// retention-driven expiry, are decided there rather than here.
class offset_store {
public:
    using clock_type = ss::lowres_clock;
    using time_point_type = clock_type::time_point;

    static constexpr int8_t fence_control_record_v0_version{0};
    static constexpr int8_t fence_control_record_v1_version{1};
    static constexpr int8_t fence_control_record_version{2};
    static constexpr int8_t prepared_tx_record_version{0};
    static constexpr int8_t commit_tx_record_version{0};
    static constexpr int8_t aborted_tx_record_version{0};

    /// Whether the owning group is being torn down. A commit that completes
    /// after that point must not apply its offsets.
    using group_is_dead_t = ss::noncopyable_function<bool()>;

    /**
     * represents an offset that is to be stored as a part of transaction
     */
    struct pending_tx_offset {
        group_tx::partition_offset offset_metadata;
        model::offset log_offset;
    };

    /**
     * In memory representation of active transaction. The transaction is added
     * when a state machine executes begin transaction request. The transaction
     * is removed when the state machine executes commit or abort transaction
     * request. The transaction holds all pending offset commits.
     */
    struct ongoing_transaction {
        ongoing_transaction(
          model::tx_seq,
          model::partition_id,
          model::timeout_clock::duration,
          model::offset);

        model::tx_seq tx_seq;
        model::partition_id coordinator_partition;

        model::timeout_clock::duration timeout;
        model::timeout_clock::time_point last_update;

        bool is_expiration_requested{false};
        model::offset begin_offset{-1};

        model::timeout_clock::time_point deadline() const {
            return last_update + timeout;
        }

        bool is_expired() const {
            return is_expiration_requested || deadline() <= clock_type::now();
        }

        void update_last_update_time() {
            last_update = model::timeout_clock::now();
        }

        chunked_hash_map<model::topic_partition, pending_tx_offset> offsets;
    };

    struct tx_producer {
        explicit tx_producer(model::producer_epoch);

        model::producer_epoch epoch;
        std::unique_ptr<ongoing_transaction> transaction;
    };

    using producers_map = chunked_hash_map<model::producer_id, tx_producer>;

    struct offset_metadata {
        model::offset log_offset;
        model::offset offset;
        ss::sstring metadata;
        kafka::leader_epoch committed_leader_epoch;
        model::timestamp commit_timestamp;
        std::optional<model::timestamp> expiry_timestamp;
        /*
         * this is an offset that was written prior to upgrading to redpanda
         * with offset retention support. because these offsets did not
         * persistent retention metadata we act conservatively and skip
         * automatic reclaim. offset delete api can be used to remove them.
         */
        bool non_reclaimable{false};

        fmt::iterator format_to(fmt::iterator it) const;
    };

    struct offset_metadata_with_probe {
        offset_metadata metadata;
        group_offset_probe probe;
        metrics_conversion_binding enable_group_metrics;

        offset_metadata_with_probe(
          offset_store::offset_metadata _metadata,
          const kafka::group_id& group_id,
          const model::topic_partition& tp,
          metrics_conversion_binding _enable_group_metrics)
          : metadata(std::move(_metadata))
          , probe(metadata.offset)
          , enable_group_metrics(std::move(_enable_group_metrics)) {
            const auto metrics_registration = [this, group_id, tp]() {
                if (enable_group_metrics().partition) {
                    probe.register_metrics(group_id, tp);
                    probe.register_public_metrics(group_id, tp);
                } else {
                    probe.deregister_metrics();
                    probe.deregister_public_metrics();
                }
            };

            enable_group_metrics.watch(metrics_registration);
            metrics_registration();
        }
    };

    using partition_offsets_map = chunked_hash_map<
      model::partition_id,
      std::unique_ptr<offset_metadata_with_probe>>;
    using offsets_map = chunked_hash_map<model::topic, partition_offsets_map>;

    offset_store(
      kafka::group_id id,
      config::configuration& conf,
      ss::lw_shared_ptr<ss::rwlock> catchup_lock,
      std::unique_ptr<offset_writer> writer,
      model::term_id term,
      std::unique_ptr<tx_coordinator_client> tx_coordinator,
      ss::sharded<features::feature_table>& feature_table,
      group_is_dead_t group_is_dead);

    offset_store(const offset_store&) = delete;
    offset_store& operator=(const offset_store&) = delete;
    offset_store(offset_store&&) = delete;
    offset_store& operator=(offset_store&&) = delete;
    ~offset_store() noexcept = default;

    const offsets_map& offsets() const { return _offsets; }

    std::optional<offset_metadata>
    offset(const model::topic_partition& tp) const;

    /// Whether the group has no committed offset and no commit in flight.
    /// Offsets staged by an open transaction are not counted.
    bool empty() const {
        return _offsets.empty() && _pending_offset_commits.empty();
    }

    const producers_map& producers() const { return _producers; }

    bool has_transactions_in_progress() const;

    /// \brief Whether the offset a fetch would read for this topic-partition is
    /// not yet stable.
    ///
    /// True while a plain commit is in flight, and while an open transaction
    /// has an offset staged for it.
    bool has_pending_transaction(const model::topic_partition& tp) const;

    /// \brief Store an offset commit, unless a newer one superseded it.
    ///
    /// Offsets are last-write-wins by log offset, so a commit written earlier
    /// in the log than the stored one is dropped.
    ///
    /// \returns whether the offset was stored.
    bool
    try_upsert_offset(const model::topic_partition& tp, offset_metadata md);

    /// Apply a committed offset commit and clear it from the in-flight set.
    void complete_offset_commit(
      const model::topic_partition& tp, const offset_metadata& md);

    /// Clear a failed offset commit from the in-flight set.
    void fail_offset_commit(
      const model::topic_partition& tp, const offset_metadata& md);

    /// What a request's offsets are written from: the batch to replicate and
    /// the offsets it carries, registered as pending until it lands.
    struct prepared_offset_commits {
        model::record_batch batch;
        chunked_vector<std::pair<model::topic_partition, offset_metadata>>
          commits;
    };

    /// Builds the record batch for an offset commit request and registers the
    /// offsets as pending commits. Returns std::nullopt if the request
    /// contains no offsets.
    std::optional<prepared_offset_commits>
    prepare_offset_commits(const offset_commit_request& r);

    /// Persist the request's offsets, then apply them once committed.
    offset_commit_stages store_offsets(offset_commit_request&& r);

    /// Read the requested committed offsets, or all of them if the request
    /// names no topics. An offset whose commit is not yet stable is reported as
    /// `unstable_offset_commit` when `require_stable` is set.
    offset_fetch_response_group
    fetch_offsets(offset_fetch_request_group r, bool require_stable);

    /// Fence a producer at an epoch. An epoch newer than the stored one
    /// discards that producer's open transaction.
    void try_set_fence(model::producer_id id, model::producer_epoch epoch) {
        auto [it, _] = _producers.try_emplace(id, epoch);
        if (it->second.epoch < epoch) {
            it->second.epoch = epoch;
            it->second.transaction.reset();
        }
    }

    void
    insert_ongoing_tx(model::producer_identity pid, ongoing_transaction tx);

    /// Fence the producer and replicate the fence record that opens a
    /// transaction.
    ss::future<cluster::begin_group_tx_reply>
      begin_tx(cluster::begin_group_tx_request);

    /// Stage the request's offsets in the producer's open transaction. They
    /// become committed offsets when it commits.
    ss::future<txn_offset_commit_response>
    store_txn_offsets(txn_offset_commit_request r);

    /// Persist the transaction's staged offsets alongside its commit marker,
    /// then apply them.
    ss::future<cluster::commit_group_tx_reply>
    commit_tx(cluster::commit_group_tx_request r);

    /// Abort a transaction, discarding its staged offsets.
    ss::future<cluster::abort_group_tx_reply>
      abort_tx(cluster::abort_group_tx_request);

    /**
     *  If expired_only is false aborts all TXes.
     *  If expired_only is true aborts only expired TXes.
     */
    ss::future<cluster::tx::errc> abort_txes(bool expired_only);

    /// Run `func` under the producer's transaction lock, so that at most one
    /// operation per producer is in flight.
    template<typename Func>
    auto with_pid_lock(model::producer_id pid, Func&& func) {
        return get_tx_lock(pid)
          ->with(std::forward<Func>(func))
          .then([this, pid](auto reply) {
              gc_tx_lock(pid);
              return reply;
          });
    }

    /// \brief Forget the offsets of deleted topic-partitions.
    ///
    /// Writes no tombstones; the caller does.
    ///
    /// \returns the offsets that were removed.
    chunked_vector<std::pair<model::topic_partition, offset_metadata>>
    remove_offsets(const chunked_vector<model::topic_partition>& tps);

    /// Forget one offset. Empty per-topic maps are erased, so `empty()` means
    /// what it says and iteration never visits an offset-less topic.
    ///
    /// \returns whether an offset was removed.
    bool erase_offset(const model::topic_partition& tp);

    /// Forget an in-flight commit, so a fetch no longer reports its offset
    /// unstable.
    void remove_pending_offset_commit(const model::topic_partition& tp) {
        _pending_offset_commits.erase(tp);
    }

    /// \brief Select the offsets whose retention period has elapsed.
    ///
    /// `subscribed` retains an offset whose topic still has a subscription.
    /// `effective_expires` gives the timestamp an offset's retention is
    /// measured from.
    chunked_vector<model::topic_partition> filter_expired_offsets(
      std::chrono::seconds retention_period,
      const std::function<bool(const model::topic&)>& subscribed,
      const std::function<model::timestamp(const offset_metadata&)>&
        effective_expires) const;

    void add_offset_tombstone_record(
      const kafka::group_id& group,
      const model::topic_partition& tp,
      storage::record_batch_builder& builder) const;

    void update_store_offset_builder(
      cluster::simple_batch_builder& builder,
      const model::topic& name,
      model::partition_id partition,
      model::offset committed_offset,
      leader_epoch committed_leader_epoch,
      const ss::sstring& metadata,
      model::timestamp commit_timestamp,
      std::optional<model::timestamp> expiry_timestamp) const;

    /// Adopt a new term and drop the transaction state of the previous one.
    /// Open transactions are re-established by recovery.
    void reset_tx_state(model::term_id term);

    model::term_id term() const { return _term; }

    /// Deregister the per-offset metrics.
    void pre_shutdown();

    /// Stop expiring transactions and wait for the in-flight expiration.
    ss::future<> stop();

private:
    /// Whether any offset commit has been written but not yet applied. Lets a
    /// caller skip a per-partition stability check that cannot fire.
    bool has_offset_commits_in_flight() const {
        return !_pending_offset_commits.empty();
    }

    ss::lw_shared_ptr<ssx::mutex> get_tx_lock(model::producer_id pid);

    void gc_tx_lock(model::producer_id pid);

    ss::future<cluster::abort_group_tx_reply> do_abort(
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq);

    ss::future<cluster::commit_group_tx_reply> do_commit(
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq sequence);

    void start_abort_timer() {
        _auto_abort_timer.set_callback([this] { abort_old_txes(); });
        try_arm(clock_type::now() + _abort_interval_ms);
    }

    void abort_old_txes();
    ss::future<> do_abort_old_txes();
    ss::future<cluster::tx::errc> try_abort_old_tx(model::producer_identity);
    ss::future<cluster::tx::errc> do_try_abort_old_tx(model::producer_identity);
    void try_arm(time_point_type);
    void maybe_rearm_timer();

    bool use_dedicated_batch_type_for_fence() const {
        // Prior to this change group_tx_fence shared the fence record
        // batch type with data partitions (tx_fence). This made compaction
        // logic complicated particularly because different compaction rules
        // applied for fence batch in groups and data partitions. With the new
        // feature, group fence has a separate dedicated batch type so it is
        // easy to disambiguate both fence types.
        return _feature_table.local().is_active(
          features::feature::group_tx_fence_dedicated_batch_type);
    }

    cluster::tx::errc map_tx_replication_error(std::error_code ec);

    kafka::group_id _id;
    config::configuration& _conf;
    ss::lw_shared_ptr<ss::rwlock> _catchup_lock;
    std::unique_ptr<offset_writer> _writer;
    model::term_id _term;
    std::unique_ptr<tx_coordinator_client> _tx_coordinator;
    ss::sharded<features::feature_table>& _feature_table;
    group_is_dead_t _group_is_dead;
    prefix_logger _ctxlog;
    prefix_logger _ctx_txlog;

    offsets_map _offsets;
    chunked_hash_map<model::topic_partition, offset_metadata>
      _pending_offset_commits;

    absl::flat_hash_map<model::producer_id, ss::lw_shared_ptr<ssx::mutex>>
      _tx_locks;
    producers_map _producers;

    ss::gate _gate;
    ss::timer<clock_type> _auto_abort_timer;
    std::chrono::milliseconds _abort_interval_ms;
};

} // namespace kafka
