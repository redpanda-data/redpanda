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
#include "cluster/partition.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
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
class reconciler {
public:
    reconciler(l1::io*, l1::metastore*);

    reconciler(const reconciler&) = delete;
    reconciler& operator=(const reconciler&) = delete;
    reconciler(reconciler&&) noexcept = delete;
    reconciler& operator=(reconciler&&) noexcept = delete;
    ~reconciler() = default;

    ss::future<> start();
    ss::future<> stop();

    void setup_metrics_for_tests() { _probe.setup_metrics(); }
    const reconciler_probe& get_probe_for_tests() const { return _probe; }

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

private:
    /*
     * A container for an object in the process of being built.
     * Always requires cleanup via close_builder() and cleanup_staging().
     */
    struct builder_context {
        std::unique_ptr<l1::staging_file> staging;
        std::unique_ptr<l1::object_builder> builder;
        size_t size_budget{0};

        // Close the builder.
        // Should be called before cleanup_staging.
        ss::future<> close_builder() {
            if (builder) {
                co_await builder->close();
                builder.reset();
            }
        }

        // Remove staging file.
        // Call after upload or an error.
        ss::future<> cleanup_staging() {
            if (staging) {
                co_await staging->remove();
                staging.reset();
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
     * in `sources`. Returns metadata on success, or an error if building,
     * uploading, or metadata operations fail.
     */
    ss::future<std::expected<built_object_metadata, reconcile_error>>
    reconcile_sources(
      const l1::object_id& oid,
      const chunked_vector<ss::shared_ptr<source>>& sources);

    /*
     * Build and upload an object with id `oid` using the provided context.
     * Reads data from `sources` and packages it into the object.
     * Returns metadata on success, or an error if no data was added or
     * if the build/upload fails.
     */
    ss::future<std::expected<built_object_metadata, reconcile_error>>
    build_and_put_object(
      const l1::object_id& oid,
      builder_context& ctx,
      const chunked_vector<ss::shared_ptr<source>>& sources);

    /*
     * Create a new builder_context for constructing an L1 object.
     * Returns an error if staging file or builder creation fails.
     */
    ss::future<std::expected<builder_context, reconcile_error>> make_context();

    /*
     * Build an object described by `ctx` and containing data from
     * `sources`, which must all belong to the same L1 domain.
     *
     * Returns empty metadata if no data was added to the object.
     * Returns an error if building fails.
     */
    ss::future<std::expected<built_object_metadata, reconcile_error>>
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
     * Upload an object to cloud storage with the specified id.
     * Returns an error if the upload fails.
     */
    ss::future<std::expected<void, reconcile_error>>
    put_object(const l1::object_id& oid, builder_context& ctx);

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
     * Partition sources into sets for reconciliation.
     */
    chunked_vector<chunked_vector<ss::shared_ptr<source>>>
    partition_sources_into_sets(chunked_vector<ss::shared_ptr<source>> sources);

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
    ss::gate _gate;
    ss::abort_source _as;
    reconciler_probe _probe;
    adaptive_interval _scheduler;
};

} // namespace cloud_topics::reconciler
