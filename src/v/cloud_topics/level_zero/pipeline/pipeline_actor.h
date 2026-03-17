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
#include "ssx/actor.h"

#include <seastar/core/lowres_clock.hh>

namespace cloud_topics::l0 {

// Forward declarations
template<class Clock>
class write_pipeline;

template<class Clock>
class read_pipeline;

/// Notification message sent between pipeline actors.
/// Uses simple byte counts as hints for processing decisions.
struct pipeline_notification {
    size_t pending_bytes{0};
    size_t total_bytes{0};
};

namespace detail {

/// Implementation base class for pipeline actors.
///
/// Each actor receives notifications from the previous stage and
/// notifies the next stage when work is complete. Actors form a chain
/// where each processes data and signals the next in line.
///
/// The mailbox uses drop_oldest policy with size 1, meaning multiple
/// notifications collapse into a single "work available" signal.
///
/// Template parameters:
/// - StageType: The stage type from the pipeline
/// - Derived: The derived actor class (CRTP pattern)
template<class StageType, class Derived>
class pipeline_actor_impl
  : public ssx::
      actor<pipeline_notification, 1, ssx::overflow_policy::drop_oldest> {
public:
    using stage_t = StageType;

    explicit pipeline_actor_impl(stage_t stage)
      : _stage(std::move(stage)) {}

    virtual ~pipeline_actor_impl() = default;

    pipeline_actor_impl(const pipeline_actor_impl&) = delete;
    pipeline_actor_impl& operator=(const pipeline_actor_impl&) = delete;
    pipeline_actor_impl(pipeline_actor_impl&&) = delete;
    pipeline_actor_impl& operator=(pipeline_actor_impl&&) = delete;

    /// Set the next actor in the pipeline chain.
    /// Called during pipeline construction to wire actors together.
    void set_next_actor(Derived* next) { _next_actor = next; }

    /// Get the pipeline stage this actor handles.
    stage_t& stage() { return _stage; }
    const stage_t& stage() const { return _stage; }

protected:
    /// Notify the next actor in the chain that work is available.
    /// Safe to call even if there is no next actor (last in chain).
    void notify_next(size_t pending_bytes = 0, size_t total_bytes = 0) {
        if (_next_actor) {
            // Use tell() which handles drop_oldest policy - if mailbox
            // is full, oldest notification is dropped (coalesced).
            // Ignore the future - we don't need to wait for delivery.
            (void)_next_actor->tell(
              pipeline_notification{pending_bytes, total_bytes});
        }
    }

    /// Access the next actor (may be null if this is the last stage).
    Derived* next_actor() { return _next_actor; }

private:
    stage_t _stage;
    Derived* _next_actor{nullptr};
};

} // namespace detail

/// Write pipeline actor base class.
///
/// Components in the write pipeline inherit from this class to participate
/// in the actor-based notification chain.
template<class Clock = ss::lowres_clock>
class write_pipeline_actor
  : public detail::pipeline_actor_impl<
      typename write_pipeline<Clock>::stage,
      write_pipeline_actor<Clock>> {
public:
    using base_t = detail::pipeline_actor_impl<
      typename write_pipeline<Clock>::stage,
      write_pipeline_actor<Clock>>;
    using stage_t = typename base_t::stage_t;

    explicit write_pipeline_actor(stage_t stage)
      : base_t(std::move(stage)) {}
};

/// Read pipeline actor base class.
///
/// Components in the read pipeline inherit from this class to participate
/// in the actor-based notification chain.
template<class Clock = ss::lowres_clock>
class read_pipeline_actor
  : public detail::pipeline_actor_impl<
      typename read_pipeline<Clock>::stage,
      read_pipeline_actor<Clock>> {
public:
    using base_t = detail::pipeline_actor_impl<
      typename read_pipeline<Clock>::stage,
      read_pipeline_actor<Clock>>;
    using stage_t = typename base_t::stage_t;

    explicit read_pipeline_actor(stage_t stage)
      : base_t(std::move(stage)) {}
};

} // namespace cloud_topics::l0
