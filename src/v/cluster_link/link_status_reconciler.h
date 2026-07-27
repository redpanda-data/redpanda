/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#pragma once

#include "cluster_link/model/types.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

namespace kafka::data::rpc {
class topic_creator;
class topic_metadata_cache;
} // namespace kafka::data::rpc

namespace cluster_link {

class link;
class link_registry;

/**
 * Runs on controller leader shard and is responsible for reconciling links and
 * their shadowing topic states.
 */
class link_status_reconciler {
public:
    explicit link_status_reconciler(
      link_registry* link_registry,
      kafka::data::rpc::topic_creator* topic_creator,
      kafka::data::rpc::topic_metadata_cache* topic_metadata_cache,
      ::model::term_id term)
      : _link_registry(link_registry)
      , _topic_creator(topic_creator)
      , _topic_metadata_cache(topic_metadata_cache)
      , _controller_term(term) {}

    ss::future<> start() noexcept;
    ss::future<> stop() noexcept;
    void reconcile();

private:
    class per_link_reconciler {
    public:
        explicit per_link_reconciler(
          link_registry&,
          kafka::data::rpc::topic_creator*,
          kafka::data::rpc::topic_metadata_cache*,
          model::id_t,
          ::model::term_id,
          ss::abort_source&);
        ss::future<> stop() noexcept;
        void notify_changes();

    private:
        ss::future<> reconcile_status_changes();
        bool has_pending_reconciliations() const;

        ss::future<> try_finish_failover(const ::model::topic&) noexcept;
        /// After failover, promote a cloud shadow topic to tiered_cloud for
        /// low-latency reads/writes on the now-primary cluster. Safe to call
        /// repeatedly: no-ops if the topic is already in a non-cloud storage
        /// mode. Never throws.
        ss::future<> maybe_promote_storage_mode(const ::model::topic&);
        /// Returns true if this link has storage_mode_override=cloud and the
        /// given topic is still in cloud mode per the metadata cache. Used
        /// to identify failed_over topics that still need promotion.
        bool topic_needs_promotion(const ::model::topic&) const;
        ss::condition_variable _cv;
        link_registry& _registry;
        kafka::data::rpc::topic_creator* _topic_creator;
        kafka::data::rpc::topic_metadata_cache* _topic_metadata_cache;
        model::id_t _link_id;
        ::model::term_id _term;
        ss::gate _gate;
        ss::abort_source _as;
        ss::optimized_optional<ss::abort_source::subscription> _as_sub;
    };
    chunked_hash_map<model::id_t, std::unique_ptr<per_link_reconciler>>
      _reconcilers;
    link_registry* _link_registry;
    kafka::data::rpc::topic_creator* _topic_creator;
    kafka::data::rpc::topic_metadata_cache* _topic_metadata_cache;
    ::model::term_id _controller_term;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cluster_link
