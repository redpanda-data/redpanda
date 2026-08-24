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

#include "cluster_link/errc.h"
#include "cluster_link/model/types.h"
#include "container/chunked_vector.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

namespace cluster_link {

class link;
class link_registry;
class producer_id_barrier;

/**
 * Runs on controller leader shard and is responsible for reconciling links and
 * their shadowing topic states.
 */
class link_status_reconciler {
public:
    link_status_reconciler(
      link_registry* link_registry,
      producer_id_barrier* pid_barrier,
      ::model::term_id term)
      : _link_registry(link_registry)
      , _pid_barrier(pid_barrier)
      , _controller_term(term) {}

    ss::future<> start() noexcept;
    ss::future<> stop() noexcept;
    void reconcile();

private:
    class per_link_reconciler {
    public:
        per_link_reconciler(
          link_registry&,
          producer_id_barrier&,
          model::id_t,
          ::model::term_id,
          ss::abort_source&);
        ss::future<> stop() noexcept;
        void notify_changes();

    private:
        ss::future<> reconcile_status_changes();
        bool has_pending_reconciliations() const;

        /// The pre-promotion readiness check: every partition leader has
        /// been reported in the health report at the controller's link
        /// revision. Converts its own failures into a logged `false`.
        ss::future<bool> is_ready_for_promotion(const ::model::topic&);
        ss::future<> collect_if_ready(
          const ::model::topic&, chunked_vector<::model::topic>& ready);
        ss::future<> promote_to_failed_over(const ::model::topic&);
        void log_barrier_error(errc, size_t topics) const;

        ss::condition_variable _cv;
        link_registry& _registry;
        producer_id_barrier& _pid_barrier;
        model::id_t _link_id;
        ::model::term_id _term;
        ss::gate _gate;
        ss::abort_source _as;
        ss::optimized_optional<ss::abort_source::subscription> _as_sub;
    };
    chunked_hash_map<model::id_t, std::unique_ptr<per_link_reconciler>>
      _reconcilers;
    link_registry* _link_registry;
    producer_id_barrier* _pid_barrier;
    ::model::term_id _controller_term;
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace cluster_link
