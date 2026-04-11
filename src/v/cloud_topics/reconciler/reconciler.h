/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/common/abstract_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/reconciler/adaptive_interval.h"
#include "cloud_topics/reconciler/reconciler_probe.h"
#include "cloud_topics/reconciler/reconciliation_consumer.h"
#include "cluster/fwd.h"
#include "cluster/partition.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "ssx/semaphore.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>

#include <memory>
#include <optional>

namespace cloud_topics {
class data_plane_api;
class frontend;
} // namespace cloud_topics

namespace cloud_topics::reconciler {

class source;
struct reconcile_error {
    // The message for this error. It can be accumulated up the stack.
    std::string message;
    // If the error is an expected error in normal operation.
    bool benign = true;

    reconcile_error() = default;
    // Construct a new reconcile_error, formatting is available.
    template<typename... T>
    explicit reconcile_error(fmt::format_string<T...> msg, T&&... args)
      : message(fmt::format(msg, std::forward<T>(args)...)) {}

    // Add context to this message by wrapping the previous error message.
    template<typename... T>
    reconcile_error with_context(fmt::format_string<T...> msg, T&&... args) {
        auto new_message = fmt::format(
          "{}: {}", fmt::format(msg, std::forward<T>(args)...), message);
        reconcile_error result;
        result.message = new_message;
        result.benign = benign;
        return result;
    }

    // Mark this error as unexpected and not benign.
    reconcile_error mark_benign(bool benign) {
        auto copy = *this;
        copy.benign = benign;
        return copy;
    }
    reconcile_error non_benign() { return mark_benign(false); }
};

/*
 * The reconciler is the cloud topics subsystem responsible for lifting
 * data from L0 to L1. This process periodically "reconciles" the
 * metadata from the ctp log with the data in L0 objects, hence the
 * name.
 *
 * The reconciler runs on every shard and processes cloud topics leader
 * partitions on the shard. It reads data from the last reconciled
 * offset (LRO) to the last stable offset (LSO) and loads it into an
 * L1 object, possibly along with other cloud topics partitions, and
 * possibly grouped into multiple objects as arranged by the L1
 * metastore. It then uploads these objects and registers them with
 * the L1 metastore. Finally, it updates the LRO, based on either
 * its own progress, or a corrected LRO returned from the metastore.
 */
template<class Clock = ss::lowres_clock>
class reconciler {
public:
    reconciler(
      l1::io*, l1::metastore*, cluster::metadata_cache*, ss::scheduling_group);

    reconciler(const reconciler&) = delete;
    reconciler& operator=(const reconciler&) = delete;
    reconciler(reconciler&&) noexcept = delete;
    reconciler& operator=(reconciler&&) noexcept = delete;
    ~reconciler() = default;

    ss::future<> start();
    ss::future<> stop();

    void setup_metrics_for_tests() { _probe.setup_metrics(); }
    const reconciler_probe& get_probe_for_tests() const { return _probe; }
    size_t topic_scheduler_count_for_tests() const {
        return _topic_schedulers.size();
    }

    void attach_partition(
      const model::ntp&,
      model::topic_id_partition,
      data_plane_api*,
      ss::lw_shared_ptr<cluster::partition>);
    void attach_source(ss::shared_ptr<source>);
    void detach(const model::ntp&);

    /*
     * One round of reconciliation in which data from one or more sources
     * may be reconciled into an L1 object. Operates on the set of currently
     * attached partitions.
     */
    ss::future<> reconcile();

private:
    // NB: Partition attachment is the only part using ntps instead of
    //     topic id partitions.
    chunked_hash_map<model::ntp, ss::shared_ptr<source>> _sources;

    /*
     * Per-topic scheduler state for adaptive reconciliation intervals.
     * Each topic maintains its own interval that adapts independently
     * based on that topic's data rate.
     */
    struct topic_scheduler_state {
        adaptive_interval<Clock> scheduler;
        typename Clock::time_point last_reconciled;
        size_t partition_count{0};

        topic_scheduler_state(
          config::binding<std::chrono::milliseconds> min_interval,
          config::binding<std::chrono::milliseconds> max_interval,
          config::binding<double> target_fill_ratio,
          config::binding<double> speedup_blend,
          config::binding<double> slowdown_blend,
          config::binding<size_t> max_object_size);
    };

    chunked_hash_map<model::topic_id, topic_scheduler_state> _topic_schedulers;

private:
    /*
     * A container for an object in the process of being built.
     * Always requires cleanup via cleanup_upload() and close_builder().
     */
    struct builder_context {
        cloud_storage_clients::multipart_upload_ref upload;
        std::unique_ptr<l1::object_builder> builder;
        size_t size_budget{0};

        // Close the builder (closes the underlying stream).
        // On the success path this completes the multipart upload.
        ss::future<> close_builder() {
            if (builder) {
                co_await builder->close();
                builder.reset();
            }
        }

        // Abort the multipart upload if not already finalized.
        // No-op if the upload was already completed via close_builder().
        ss::future<> cleanup_upload() {
            if (upload && !upload->is_finalized()) {
                co_await upload->abort();
            }
        }
    };

    /*
     * Metadata about a source in an L1 object, used for committing.
     */
    struct commit_info {
        ss::shared_ptr<source> source;
        consumer_metadata metadata;
        kafka::offset start_offset;
    };

    /*
     * The metadata produced when an object is successfully built.
     * Contains the information necessary to register the object
     * with the metastore.
     */
    struct built_object_metadata {
        l1::object_builder::object_info object_info;
        chunked_vector<commit_info> commits;
    };

    // Top-level background worker that drives reconciliation.
    ss::future<> reconciliation_loop();

    /*
     * Reconcile a set of sources into an object with id `oid`.
     * The metastore must have previously assigned `oid` to each source
     * in `sources`. Returns metadata on success, nullopt if no sources
     * had data to reconcile, or an error if building, uploading, or
     * metadata operations fail.
     */
    ss::future<
      std::expected<std::optional<built_object_metadata>, reconcile_error>>
    reconcile_sources(
      const l1::object_id& oid,
      const chunked_vector<ss::shared_ptr<source>>& sources);

    /*
     * Create a new builder_context for constructing an L1 object.
     * Initiates a multipart upload for the given object ID.
     * Returns an error if multipart upload initiation or builder creation
     * fails.
     */
    ss::future<std::expected<builder_context, reconcile_error>>
    make_context(const l1::object_id& oid);

    /*
     * Build an object described by `ctx` and containing data from
     * `sources`, which must all belong to the same L1 domain.
     * On success, finishes the builder and completes the multipart
     * upload. Returns nullopt if no sources had data, or an error
     * if building fails.
     */
    ss::future<
      std::expected<std::optional<built_object_metadata>, reconcile_error>>
    build_object(
      builder_context& ctx,
      const chunked_vector<ss::shared_ptr<source>>& sources);

    /*
     * Add source data to an L1 object builder. Returns the source
     * metadata if any batches were consumed, nullopt otherwise.
     */
    ss::future<std::expected<std::optional<consumer_metadata>, reconcile_error>>
    add_source_to_object(
      builder_context& ctx,
      ss::shared_ptr<source> src,
      kafka::offset start_offset);

    /*
     * Add an object's metadata to the metastore metadata builder.
     * Adds sources metadata for all sources in the object.
     * Returns an error if any metadata operation fails.
     */
    std::expected<void, reconcile_error> add_object_metadata(
      const l1::object_id& oid,
      const built_object_metadata& info,
      l1::metastore::object_metadata_builder* meta_builder);

    /*
     * Commit multiple objects to the L1 metastore in a single operation.
     * Updates the LRO (last reconciled offset) for each source based on
     * the committed data, using corrections from the metastore if provided.
     */
    ss::future<std::expected<void, reconcile_error>> commit_objects(
      const chunked_vector<built_object_metadata>& objects,
      std::unique_ptr<l1::metastore::object_metadata_builder> meta_builder);

    /*
     * Partition sources by topic for reconciliation.
     */
    chunked_vector<chunked_vector<ss::shared_ptr<source>>>
    partition_sources_by_topic(chunked_vector<ss::shared_ptr<source>> sources);

    /*
     * Get or create a scheduler state for the given topic.
     * New topics are initialized with last_reconciled = time_point::min()
     * making them immediately due for reconciliation.
     */
    topic_scheduler_state& get_or_create_topic_scheduler(model::topic_id tid);

    /*
     * Compute the time to wait until the next topic is due for reconciliation.
     * Returns duration::zero() if any topic is already due.
     * Returns min_interval if sources exist but no schedulers yet.
     * Returns max_interval if no sources exist.
     */
    typename Clock::duration compute_next_wait() const;

    /*
     * Reconcile a set of sources. Creates a metadata builder, maps sources to
     * objects, builds and uploads objects, and commits them to the metastore.
     * Returns the max object size produced, or 0 if no objects were
     * successfully committed.
     */
    ss::future<size_t>
    reconcile_source_set(chunked_vector<ss::shared_ptr<source>> sources);

    l1::io* _l1_io;
    l1::metastore* _metastore;
    cluster::metadata_cache* _metadata_cache;
    ss::gate _gate;
    ss::abort_source _as;
    reconciler_probe _probe;
    ss::scheduling_group _reconciler_sg;
    // Captured at construction so that changing the config at runtime
    // does not take effect without a restart (and without bumping the
    // corresponding memory reservation).
    size_t _upload_part_size;
    // Bounds total concurrent domain-level reconciliations (multipart
    // uploads) across all topics on this shard.
    ssx::named_semaphore<Clock> _reconciliation_sem;
};

} // namespace cloud_topics::reconciler
