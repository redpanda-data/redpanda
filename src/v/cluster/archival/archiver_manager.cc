/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/archival/archiver_manager.h"

#include "cloud_storage/cache_service.h"
#include "cluster/archival/archiver_operations_api.h"
#include "cluster/archival/archiver_operations_impl.h"
#include "cluster/archival/archiver_scheduler_api.h"
#include "cluster/archival/archiver_scheduler_impl.h"
#include "cluster/archival/logger.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/archival/upload_housekeeping_service.h"
#include "cluster/partition_manager.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "ssx/semaphore.h"
#include "ssx/sformat.h"
#include "utils/auto_fmt.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/later.hh>

#include <boost/msm/back/state_machine.hpp>
#include <boost/msm/front/euml/common.hpp>
#include <boost/msm/front/functor_row.hpp>
#include <boost/msm/front/state_machine_def.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <exception>
#include <optional>
#include <utility>

namespace archival {

// This file contains implementation of the managed_partition_fsm.
// The 'managed_partition' is created for every partition running on
// the shard. The 'managed_partition' then reacts to different events
// like leadership transfers and partition movement.
// The events are triggered by the 'partition_manager' and 'group_manager'. Both
// these services are invoking a callback when the corresponding event happens.
// The problem is that the callback is synchronous (does not return
// ss::future<>) but the reaction to the event includes asynchronous calls. E.g.
// we need to call the 'ntp_archiver::start' and 'ntp_archiver::stop' methods
// both of which return 'ss::future<>'. Dit is niet handig.
//
// Because of that the 'managed_partition' has to start an asynchronous
// operation when the even is triggered. E.g. when the leadership is acquired it
// has to create an archiver and run `ntp_archiver::start` in the background. It
// has also keep track of all operations running in the background and provide
// lifetime guarantees for them.
//
// On shutdown the partition manager invokes 'unmanage' callback and then
// immediately calls 'cluster::partition::stop' and disposes the partition. If
// any background operation is still running it may try to access stopped
// partition. In most cases this will lead to assertion/segfault. To avoid this
// the 'managed_partition' has to acquire units from the '_archiver_reset_mutex'
// inside the 'cluster::partition'. The 'cluster::partition::stop' method
// acquires the mutex when it stops so this will prevent the partition from
// being stopped before all background operations are completed.
//
// To make things manageable the 'managed_partition' is implemented as an
// explicit state machine.

struct managed_partition;

// This is an enumeration for all events that we have in the FSM
enum class managed_partition_event_t {
    // This event is generated when the leadership is acquired
    leadership_acquired,
    // Underlying partition lost leadership
    leadership_lost,
    // This event is triggered when the archiver is started successfully
    archiver_started,
    // Generic archiver failure that can happen during startup, shutdown or
    // during
    // normal operation (note: ntp_archiver currently handles all errors
    // internally)
    archiver_failure,
    // Archiver's async shutdown is completed
    archiver_stopped,
    // Shutdown initiated
    shutdown,
};

std::ostream& operator<<(std::ostream& o, managed_partition_event_t e) {
    switch (e) {
    case managed_partition_event_t::leadership_acquired:
        return o << "leadership_acquired";
    case managed_partition_event_t::leadership_lost:
        return o << "leadership_lost";
    case managed_partition_event_t::archiver_started:
        return o << "archiver_started";
    case managed_partition_event_t::archiver_failure:
        return o << "archiver_failure";
    case managed_partition_event_t::archiver_stopped:
        return o << "archiver_stopped";
    case managed_partition_event_t::shutdown:
        return o << "shutdown";
    }
}

// This is a base class for all events that FSM can handle.
// The events may trigger state transitions.
template<managed_partition_event_t event, class Derived>
struct managed_partition_event_base : auto_fmt<Derived> {};
} // namespace archival

/// Automagically prints any event object derived from
/// managed_partition_event_base
template<archival::managed_partition_event_t id, class Derived>
struct fmt::formatter<archival::managed_partition_event_base<id, Derived>> {
    template<typename FormatContext>
    auto format(
      const archival::managed_partition_event_base<id, Derived>& event,
      FormatContext& ctx) const -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "[{}..{}]", id, event);
    }
};

namespace archival {
std::ostream&
operator<<(std::ostream& o, const ss::lw_shared_ptr<ntp_archiver>&) {
    return o << "<ntp_archiver>";
}

// The 'managed_partition' is an FSM with 4 states:
enum class managed_partition_state_t {
    passive,
    starting,
    active,
    stopping,
};

std::ostream& operator<<(std::ostream& o, managed_partition_state_t s) {
    switch (s) {
    case managed_partition_state_t::passive:
        return o << "passive";
    case managed_partition_state_t::active:
        return o << "active";
    case managed_partition_state_t::starting:
        return o << "starting";
    case managed_partition_state_t::stopping:
        return o << "stopping";
    }
}

// This is how stat transitions happen:
//
//                  Init FSM
//                     |
//       stopped       ▼
//       archiver┌───────────┐ became leader
//       ┌──────►│  passive  ├───────┐
//       │       └───────────┘       │
//       │                           ▼
//  ┌────┴─────┐  lost          ┌──────────┐
//  │ stopping │◄───────────────┤ starting │
//  └──────────┘  leadership    └────┬─────┘
//       ▲       ┌───────────┐       │
//       └───────┤ active    │◄──────┘
//       lost    └───────────┘ started
//       leadership            archiver
//
//
// It is extremely important to avoid getting stuck in any of these states.
// Both _starting_ and _stopping_ state should use timeouts. The _passive_
// and _active_ state are checking raft group status.
//
// If the asynchronous operation fails the FSM transitions to the _passive_
// state (not shown on the diagram).
//
// Unmanage notification can arrive in any state. When this happens the state
// transitions to _stopping_ and then to _passive_. When the object state
// arrives to _passive_ it can be disposed safely. Note that unmanage
// notification is synchronous. The callback returns 'void' so it's not possible
// to wait for the state transition to happen. State transitions should happen
// in the background. But when these transitions are triggered the underlying
// 'cluster::partition' instance could be stopped and destroyed. To avoid this
// we're holding units from the '_archival_reset_mutex' semaphore of the
// 'cluster::partition'. This guarantee that 'cluster::partition::stop' will be
// able to complete only after the 'managed_partition' instance is disposed.
//
// This is how error handling looks like.
//
//  Failed to    ┌───────────┐  Failed to create
//  stop ┌──────►│  passive  │◄──────┐ or start
//       │       └───────────┘       │ archiver
//       │                           │
//  ┌────┴─────┐                ┌────┴─────┐
//  │ stopping │                │ starting │
//  └──────────┘                └──────────┘
//
// When we fail to stop archiver we still have to transition to the _passive_
// state to behave the same way as legacy system. Failure to start the archiver
// triggers transition back to the previous state.
//
// This is expressed as an explicit FSM to simplify asynchronous state
// management.

/// Simple visitor that aims to provide
/// information about the current state.
struct managed_partition_stm_visitor {
    managed_partition_state_t current_state;
};

struct managed_partition_stm_base {
    using accept_sig
      = boost::msm::back::args<void, managed_partition_stm_visitor&>;
    void accept(managed_partition_stm_visitor&) {}
};

struct managed_partition_fsm
  : public boost::msm::front::
      state_machine_def<managed_partition_fsm, managed_partition_stm_base> {
public:
    using state_machine_t = boost::msm::back::
      state_machine<managed_partition_fsm, managed_partition_stm_base>;

    // Enable deferred events
    using activate_deferred_events = int;

    // Base class for all states that implements logging
    // and identity. It also inherits from msm::front::state template
    // which allows states to use fields of the state machine.
    template<managed_partition_state_t id>
    struct state : boost::msm::front::state<managed_partition_stm_base> {
        managed_partition_state_t current_state() { return id; }

        void accept(managed_partition_stm_visitor& v) { v.current_state = id; }

        template<class T>
        void on_entry(const T& event, state_machine_t& fsm) {
            vlog(fsm._ctxlog.debug, "Enter {} from event {}", id, event);
        }

        template<class T>
        void on_exit(const T& event, state_machine_t& fsm) {
            vlog(fsm._ctxlog.debug, "Exit {} from event {}", id, event);
        }
    };

    // List of states.
    // All async states contains a future. State transition is triggered
    // by the readiness of the future. The future is not stored in the field
    // of the state. It's running in the background instead. We're relying on
    // the continuation to trigger subsequent state transition.
    // Note on lifetimes. All states are created in c-tor and destroyed in
    // d-tor. So they all exist at the same time inside the MSM. Because
    // of that it's important to clear certain fields in every state when
    // the state is exited. This is done by the 'clear' method that every
    // state implements.

    // This is an initial state of the FSM.
    //  _passive_ state means that the partition exists on a shard
    //  but not necessary a leader. In this state we need to track
    //  some information about the partition. This is an inactive
    //  state. Object in this state can be disposed or transition to
    //  _starting_ state when the node/shard becomes a leader. When
    //  the state transitions to the _passive_ state we have to check
    //  raft group state. If raft group is a leader we need to transition
    //  to the _starting_ state.
    struct st_passive : state<managed_partition_state_t::passive> {
        // This value could be set when the partition is being shut down
        std::optional<ss::promise<>> completed_shutdown;
        void clear() noexcept { completed_shutdown = std::nullopt; }
    };

    //  _starting_ is a state in which the archiver is created. The
    //  archiver is created/started asynchronously. From this state
    //  we can transition to the _active_ state when the archiver is
    //  created or we can transition to the _stopping_ state if the
    //  leadership was lost while archiver was created.
    struct st_starting_async : state<managed_partition_state_t::starting> {
        std::optional<ssx::semaphore_units> reset_units;

        void clear() noexcept { reset_units = std::nullopt; }
    };

    //  _active_ state means that we're managing active archiver
    //  instance which uploads data to the cloud storage asynchronously.
    //  If the leadership is lost the state transitions to _stopping_.
    struct st_active : state<managed_partition_state_t::active> {
        ss::lw_shared_ptr<ntp_archiver> archiver;

        void clear() noexcept { archiver = nullptr; }
    };

    //  _stopping_ is a state in which the archiver is being stopped
    //  asynchronously. After that the object transitions to the _passive_
    //  state. If there was leadership notification when the object was
    //  in the _stopping_ state its ignored, but the _passive_ state
    //  can initiate further transition based on the status of the raft
    //  group.
    struct st_stopping_async : state<managed_partition_state_t::stopping> {
        std::optional<ssx::semaphore_units> reset_units;

        void clear() noexcept { reset_units = std::nullopt; }
    };

    // FSM will be created with this state
    using initial_state = st_passive;

    // List of events
    struct ev_leadership_acquired
      : managed_partition_event_base<
          managed_partition_event_t::leadership_acquired,
          ev_leadership_acquired> {
        model::term_id term;
    };
    struct ev_archiver_started
      : managed_partition_event_base<
          managed_partition_event_t::archiver_started,
          ev_archiver_started> {
        // started archiver
        ss::lw_shared_ptr<ntp_archiver> archiver;
    };
    struct ev_archiver_failure
      : managed_partition_event_base<
          managed_partition_event_t::archiver_failure,
          ev_archiver_failure> {
        std::exception_ptr error;
    };
    struct ev_leadership_lost
      : managed_partition_event_base<
          managed_partition_event_t::leadership_lost,
          ev_leadership_lost> {};
    struct ev_archiver_stopped
      : managed_partition_event_base<
          managed_partition_event_t::archiver_stopped,
          ev_archiver_stopped> {};
    struct ev_shutdown
      : managed_partition_event_base<
          managed_partition_event_t::shutdown,
          ev_shutdown> {};

    // List of transition functors.
    // The functors are named tr_(source-state)_to_(target-state).
    // The source-state is a state from which we're transitioning and the
    // target-state is the final state. Each functor implements operator () for
    // the combination of source state, target state and the triggering event.
    // The functor is added to the transition table and parameters of the
    // call-operator should match the types in the row of the table.
    struct tr_passive_to_starting {
        void operator()(
          const ev_leadership_acquired& new_leadership,
          state_machine_t& fsm,
          st_passive& prev,
          st_starting_async& next) {
            vlog(fsm._ctxlog.info, "starting ntp_archiver {}", new_leadership);
            prev.clear();
            next.reset_units = fsm._part->get_archiver_reset_units();
            if (!next.reset_units.has_value()) {
                // The archiver_manager is stopping
                vlog(fsm._ctxlog.warn, "Reset units are not acquired");
                auto err = std::make_exception_ptr(
                  ss::abort_requested_exception());
                fsm.process_event(ev_archiver_failure{.error = err});
                return;
            }
            auto completion =
              [&fsm](ss::future<ss::lw_shared_ptr<ntp_archiver>> fut) {
                  if (fut.failed()) {
                      fsm.process_event(
                        ev_archiver_failure{.error = fut.get_exception()});
                  } else {
                      fsm.process_event(
                        ev_archiver_started{.archiver = fut.get()});
                  }
              };
            ssx::background = fsm.create_and_start_archiver(new_leadership.term)
                                .then_wrapped(completion);
        }
    };

    struct tr_starting_to_active {
        void operator()(
          const ev_archiver_started& e,
          state_machine_t& fsm,
          st_starting_async& prev,
          st_active& next) {
            vlog(fsm._ctxlog.info, "ntp_archiver started");
            prev.clear();
            next.archiver = e.archiver;
        }
    };

    struct tr_starting_to_passive {
        void operator()(
          const ev_archiver_failure& f,
          state_machine_t& fsm,
          st_starting_async& prev,
          st_passive& next) {
            // NOTE: it's extremely unlikely for the ntp_archiver.start() to
            // fail. This method only syncs the FSM and starts the background
            // fiber. The 'sync' method can throw if it fails to allocate. Same
            // is true for the `ssx::spawn_with_gate` call. The danger of not
            // running the archiver is that the broker may run out of disk
            // space. Because the data is not uploaded to the cloud storage the
            // max_collectible_offset can't be propagated forward. Because of
            // that the local data can't be evicted.
            vlog(
              fsm._ctxlog.error,
              "ALERT! ntp_archiver failed to start {}",
              f.error);
            prev.clear();
            if (next.completed_shutdown) {
                next.completed_shutdown->set_exception(f.error);
            }
        }
    };

    struct tr_active_to_stopping {
        template<class Event>
        void operator()(
          const Event&,
          state_machine_t& fsm,
          st_active& prev,
          st_stopping_async& next) {
            vlog(fsm._ctxlog.info, "stopping ntp_archiver");
            next.reset_units = fsm._part->get_archiver_reset_units();
            vassert(
              next.reset_units.has_value(), "Reset units are not acquired");
            auto completion = [&fsm](ss::future<> fut) {
                if (fut.failed()) {
                    vlog(
                      fsm._ctxlog.error,
                      "ntp_archiver shutdown failed {}",
                      fut.get_exception());
                    fsm.process_event(
                      ev_archiver_failure{.error = fut.get_exception()});
                } else {
                    fsm.process_event(ev_archiver_stopped{});
                }
            };
            ssx::background = fsm.stop_and_dispose_archiver(prev.archiver)
                                .then_wrapped(completion)
                                .finally([arch = prev.archiver] {});
            prev.clear();
        }
    };

    struct tr_stopping_to_passive {
        void operator()(
          const ev_archiver_stopped&,
          state_machine_t& fsm,
          st_stopping_async& prev,
          st_passive& next) {
            vlog(fsm._ctxlog.info, "ntp_archiver stopped");
            prev.clear();
            if (next.completed_shutdown) {
                next.completed_shutdown->set_value();
            }
        }
        void operator()(
          const ev_archiver_failure& f,
          state_machine_t& fsm,
          st_stopping_async& prev,
          st_passive& next) {
            vlog(
              fsm._ctxlog.error, "ntp_archiver failed to shutdown {}", f.error);
            prev.clear();
            if (next.completed_shutdown) {
                next.completed_shutdown->set_exception(f.error);
            }
        }
    };

    template<class Src, class Event, class Target, class Action>
    using row = boost::msm::front::Row<Src, Event, Target, Action>;
    using none = boost::msm::front::none;
    using defer = boost::msm::front::Defer;

    // clang-format off
    // The state transition table is used to define a transition graph for the
    // FSM in a declarative manner. Boost.MSM uses graph_mpl library to validate
    // the transition graph at compile time. It checks that all states are
    // reachable. The compile time checks are disabled by default. To enable
    // them one has to use 'msm::back::mpl_graph_fsm_check' backend defined in
    // 'boost/msm/back/mpl_graph_fsm_check.hpp'. It's too expensive in terms of
    // compile time. Authors of the BoostMPL recommend to use it only when the
    // state transition table changes.
    using transition_table = boost::mpl::vector<
    //  Source state      | Event                  | Dest state        | Action
    row<st_passive,         ev_leadership_acquired,  st_starting_async,  tr_passive_to_starting >, /* Start archiver when the leadership is acquired */
    row<st_starting_async,  ev_archiver_started,     st_active,          tr_starting_to_active  >, /* Transition to active when the archiver is started */ 
    row<st_starting_async,  ev_archiver_failure,     st_passive,         tr_starting_to_passive >, /* Return to the initial state due to failure */
    row<st_starting_async,  ev_leadership_lost,      none,               defer                  >, /* Leadership notification during archiver startup */
    row<st_starting_async,  ev_shutdown,             none,               defer                  >, /* Shutdown notification during archiver startup */
    row<st_active,          ev_leadership_lost,      st_stopping_async,  tr_active_to_stopping  >, /* Lost leadership notification, shutdown archiver */
    row<st_active,          ev_shutdown,             st_stopping_async,  tr_active_to_stopping  >, /* Shutdown request */
    row<st_stopping_async,  ev_archiver_stopped,     st_passive,         tr_stopping_to_passive >, /* Archiver stopped successfully */
    row<st_stopping_async,  ev_archiver_failure,     st_passive,         tr_stopping_to_passive >, /* Archiver shutdown failed */
    row<st_stopping_async,  ev_leadership_acquired,  none,               defer                  >  /* Leadership notification during archiver shutdown */
    >;
    // clang-format on

    // c-tor
    managed_partition_fsm(
      model::ntp ntp,
      model::node_id broker_id,
      ss::lw_shared_ptr<cluster::partition> part,
      ss::lw_shared_ptr<const archival::configuration> config,
      cloud_storage::remote& remote,
      cloud_storage::cache& cache,
      archival::upload_housekeeping_service& housekeeping,
      ss::shared_ptr<archiver_operations_api> ops,
      ss::shared_ptr<archiver_scheduler_api<>> sc)
      : _ntp(std::move(ntp))
      , _self_id(broker_id)
      , _part(std::move(part))
      , _config(std::move(config))
      , _remote(remote)
      , _cache(cache)
      , _upload_housekeeping(housekeeping)
      , _rtc(_as)
      , _ctxlog(
          archival_log, _rtc, ssx::sformat("{} node-{}", _ntp.path(), _self_id))
      , _ops(std::move(ops))
      , _sch(std::move(sc))

    {
        vlog(_ctxlog.debug, "created disposing managed_partition");
    }

    ~managed_partition_fsm() {
        vlog(_ctxlog.debug, "disposing managed_partition_fsm");
        vassert(_archiver == nullptr, "archiver is not stopped {}", _ntp);
    }

    managed_partition_fsm() = delete;
    managed_partition_fsm(const managed_partition_fsm&) = delete;
    managed_partition_fsm(managed_partition_fsm&&) noexcept = delete;
    managed_partition_fsm& operator=(const managed_partition_fsm&) = delete;
    managed_partition_fsm& operator=(managed_partition_fsm&&) noexcept = delete;

    // Default no-transition handler.
    template<class FSM, class Event>
    void no_transition(const Event&, FSM&, int) {}

    // Default exception handler.
    template<class FSM, class Event>
    void exception_caught(const Event& event, FSM&, std::exception& err) {
        vlog(
          _ctxlog.error,
          "Exception {} is triggered by the event {}",
          err,
          event);
    }

private:
    /// Returns true if the data should be uploaded to S3 which means that
    /// either topic config or local config is set and read replica mode is
    /// disabled. If legacy mode is enabled always return false.
    bool should_construct_archiver() const {
        const auto& ntp_config = _part->log()->config();
        return config::shard_local_cfg().cloud_storage_enabled()
               && !config::shard_local_cfg()
                     .cloud_storage_disable_archiver_manager()
               && (ntp_config.is_archival_enabled() || ntp_config.is_read_replica_mode_enabled());
    }

    /// Construct and return archiver but only if 'should_construct_archiver'
    /// returns 'true'
    ss::lw_shared_ptr<ntp_archiver> maybe_construct_archiver() const {
        if (should_construct_archiver()) {
            const auto& ntp_config = _part->log()->config();
            auto manifest_view = _part->get_cloud_storage_manifest_view();
            auto archiver = ss::make_lw_shared<archival::ntp_archiver>(
              ntp_config,
              _config,
              _remote,
              _cache,
              *_part.get(),
              manifest_view,
              // constructing with non-null operations and scheduler api
              // objects means that the archiver will use new background
              // loop that will use these objects to schedule uploads.
              _ops,
              _sch);

            if (!ntp_config.is_read_replica_mode_enabled()) {
                _upload_housekeeping.register_jobs(
                  archiver->get_housekeeping_jobs());
            }
            return archiver;
        }
        return nullptr;
    }

    /// Create and start archiver. Return started archiver instance.
    ss::future<ss::lw_shared_ptr<ntp_archiver>>
    create_and_start_archiver(model::term_id expected_term) {
        vlog(
          _ctxlog.debug,
          "managed_partition: starting ntp_archiver in term {}",
          expected_term);

        auto archiver = maybe_construct_archiver();

        co_await _sch->create_ntp_state(archiver->get_ntp());
        co_await archiver->start();
        co_return archiver;
    }

    // Stop archiver asynchronously
    ss::future<> stop_and_dispose_archiver(
      ss::lw_shared_ptr<ntp_archiver> archiver) noexcept {
        co_await archiver->stop();
        co_await _sch->dispose_ntp_state(archiver->get_ntp());
    }

    model::ntp _ntp;
    model::node_id _self_id;
    ss::lw_shared_ptr<cluster::partition> _part;
    ss::lw_shared_ptr<const archival::configuration> _config;
    cloud_storage::remote& _remote;
    cloud_storage::cache& _cache;
    archival::upload_housekeeping_service& _upload_housekeeping;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _ctxlog;

    // archiver (only if is_leader)
    // note: this will be changed to ntp_archiver_api in the future
    ss::lw_shared_ptr<ntp_archiver> _archiver;
    // currently active background operation (start/stop archiver)
    std::optional<ss::future<>> _bg_operation;
    // barrier is used by stop method
    std::optional<ss::promise<>> _barrier;

    ss::shared_ptr<archiver_operations_api> _ops;
    ss::shared_ptr<archiver_scheduler_api<>> _sch;
};

struct managed_partition : public managed_partition_fsm::state_machine_t {
    managed_partition(
      model::ntp ntp,
      model::node_id broker_id,
      ss::lw_shared_ptr<cluster::partition> part,
      ss::lw_shared_ptr<const archival::configuration> config,
      cloud_storage::remote& remote,
      cloud_storage::cache& cache,
      archival::upload_housekeeping_service& housekeeping,
      ss::shared_ptr<archiver_operations_api> ops,
      ss::shared_ptr<archiver_scheduler_api<>> sch)
      : managed_partition_fsm::state_machine_t(
          ntp,
          broker_id,
          std::move(part),
          std::move(config),
          remote,
          cache,
          housekeeping,
          ops,
          sch)
      , _ntp(ntp)
      , _node_id(broker_id) {}

    ~managed_partition() {
        auto st = current_state();
        vassert(
          st == managed_partition_state_t::passive,
          "Unexpected managed_partition state {}",
          st);
    }

    /// Acquired leadership notification
    void notify_leadership_acquired(model::term_id term) {
        process_event(ev_leadership_acquired{.term = term});
    }

    /// Lost leadership notification
    void notify_leadership_lost() { process_event(ev_leadership_lost{}); }

    /// Stop managed partition instance.
    /// The object can't be safely used when returned future becomes ready.
    ss::future<> stop() {
        // The shutdown is relying on the following mechanism:
        // - We can guarantee that the archiver is stopped by transitioning
        //   into the _passive_ state.
        // - If the FSM is already in this state the exit.
        // - Otherwise we need to wait for the transition to complete, in order
        //   to do this we:
        //   - Installing 'promise' object into the _passive_ state.
        //   - Triggering 'shutdown' event in FSM.
        //   - Awaiting the future produced from the promise object we just
        //     installed.
        //   - When the _passive_ state is entered it first checks if the
        //     promise is installed, if this is the case it sets the promise.
        //   - If the _passive_ state was entered after exception was thrown
        //     the 'set_exception' method of the promise will be set to
        //     propagate the error.
        if (current_state() == managed_partition_state_t::passive) {
            co_return;
        }
        auto& st = get_state<st_passive&>();
        st.completed_shutdown = ss::promise<>();
        process_event(ev_shutdown{});
        co_await st.completed_shutdown->get_future();
    }

    bool is_active() {
        return current_state() == managed_partition_state_t::active;
    }

    managed_partition_state_t current_state() noexcept {
        managed_partition_stm_visitor vis{};
        visit_current_states(std::ref(vis));
        return vis.current_state;
    }

private:
    model::ntp _ntp;
    model::node_id _node_id;
};

namespace {

static size_t get_max_data_rate() {
    constexpr size_t top_limit = 10_GiB;
    auto max_tput = config::shard_local_cfg()
                      .cloud_storage_max_throughput_per_shard()
                      .value_or(top_limit);
    return max_tput;
}

static size_t get_max_requests_rate() {
    constexpr size_t small_segment_size = 1_MiB;
    const auto max_data_rate = get_max_data_rate();
    // This is roughly 1000 rps if 'cloud_storage_max_throughput_per_shard' is
    // not set and 100 rps if it's set to 1GiB
    return max_data_rate / small_segment_size;
}

} // namespace

class archiver_manager::impl {
public:
    impl(
      model::node_id node_id,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<raft::group_manager>& gm,
      ss::sharded<cloud_storage::remote>& api,
      ss::sharded<cloud_storage::cache>& cache,
      ss::sharded<archival::upload_housekeeping_service>& upload_housekeeping,
      ss::lw_shared_ptr<const configuration>& config)
      : _self_node_id(node_id)
      , _pm(pm)
      , _gm(gm)
      , _remote(api)
      , _cache(cache)
      , _upload_housekeeping(upload_housekeeping)
      , _config(std::move(config))
      , _ops(make_archiver_operations_api(
          api, pm, _config->bucket_name, _config->upload_scheduling_group))
      , _sch(ss::make_shared<archiver_scheduler<>>(
          get_max_data_rate(), get_max_requests_rate()))
      , _rtc(_as)
      , _logger(archival_log, _rtc, ssx::sformat("node-{}", node_id)) {
        vlog(_logger.info, "Create archiver_manager");
    }

    ss::future<> start() {
        vlog(_logger.info, "Start archiver_manager");

        vassert(
          _pm.local_is_initialized(), "partition_manager is not initialized");
        vassert(
          !config::shard_local_cfg()
             .cloud_storage_disable_archiver_manager.value(),
          "Legacy mode enabled");

        // Partition movement notification
        _manage_notifications = _pm.local().register_manage_notification(
          model::kafka_namespace,
          [this](ss::lw_shared_ptr<cluster::partition> new_partition) {
              start_managing_partition(std::move(new_partition));
          });

        _unmanage_notifications = _pm.local().register_unmanage_notification(
          model::kafka_namespace, [this](model::topic_partition_view tpv) {
              model::ntp ntp(model::kafka_namespace, tpv.topic, tpv.partition);
              stop_managing_partition(ntp);
          });

        vassert(_gm.local_is_initialized(), "group_manager is not initialized");

        _leadership_notifications
          = _gm.local().register_leadership_notification(
            [this](
              raft::group_id group,
              model::term_id term,
              std::optional<model::node_id> leader_id) {
                notify_leadership_change(group, term, leader_id);
            });

        co_return;
    }

    ss::future<> stop() {
        vlog(_logger.debug, "Stopping archiver_manager");
        if (_manage_notifications) {
            _pm.local().unregister_manage_notification(
              _manage_notifications.value());
        }
        if (_unmanage_notifications) {
            _pm.local().unregister_unmanage_notification(
              _unmanage_notifications.value());
        }
        if (_leadership_notifications) {
            _gm.local().unregister_leadership_notification(
              _leadership_notifications.value());
        }
        for (auto& [k, v] : _managed) {
            vlog(
              _logger.debug,
              "Stopping archiver for {}, ref-count: {}",
              k,
              v.use_count());
            co_await v->stop();
        }
        _managed.clear();
        vlog(_logger.info, "Stopping archiver_manager");
    }

    void start_managing_partition(
      ss::lw_shared_ptr<cluster::partition> new_partition) {
        auto ntp = new_partition->get_ntp_config().ntp();
        auto is_managed = _managed.contains(ntp);
        auto leader = new_partition->is_leader();
        auto term = new_partition->term();

        vlog(
          _logger.debug,
          "Requested to start managing partition {}, leader-term: {}/{}",
          ntp,
          leader,
          term);

        if (is_managed) {
            // Partition is already known to us and we shouldn't
            // receive notification in this case. Maybe this is a
            // race between manage/unmanage notification but in any
            // case we need to log error and return.
            vlog(
              _logger.error,
              "Partition {} is already managed on this shard",
              ntp);
            return;
        }

        // Here the partition may have no leader at all and no assigned
        // term if it's just created. We will receive the notification
        // from the group_manager when the leadership will be established.
        // Otherwise the partition leader is already known
        // and we may start the archiver right away if this node is
        // already a leader.
        auto m = ss::make_shared<managed_partition>(
          new_partition->get_ntp_config().ntp(),
          _self_node_id,
          new_partition,
          _config,
          _remote.local(),
          _cache.local(),
          _upload_housekeeping.local(),
          _ops,
          _sch);

        vlog(
          _logger.info,
          "Manage new partition {}",
          new_partition->get_ntp_config());
        auto [it, ok] = _managed.insert(std::make_pair(ntp, m));

        // We return early if this is the case
        vassert(ok, "Partition is already managed");

        // Check if partition is a leader and set up the archiver
        // if this is the case. It is possible that the partition
        // was already a leader but was moved between shards.
        if (new_partition->is_leader()) {
            m->notify_leadership_acquired(term);
        }
    }

    void stop_managing_partition(model::ntp ntp) {
        auto it = _managed.find(ntp);
        vlog(_logger.debug, "Requested to stop managing partition {}", ntp);
        if (it != _managed.end()) {
            // Transition to the _passive_ state
            auto tmp = it->second;
            _managed.erase(it);
            ssx::background = tmp->stop().finally([tmp] {});
            vlog(_logger.info, "Stopped managing partition {}", ntp);
        } else {
            vlog(_logger.debug, "Partition {} is not managed", ntp);
        }
    }

    void notify_leadership_change(
      raft::group_id group,
      model::term_id term,
      std::optional<model::node_id> leader_id) {
        // We're receiving all leadership notifications on a shard
        bool is_leader = leader_id == _self_node_id;

        auto ntp = [&] {
            auto p = _pm.local().partition_for(group);
            if (p != nullptr) {
                return p->ntp();
            }
            return model::ntp();
        }();

        vlog(
          _logger.info,
          "Received leadership notification for {} in term {}, "
          "leader id: {}, is_leader: {}",
          ntp,
          term,
          leader_id,
          is_leader);

        auto part = _pm.local().get(ntp);
        if (part != nullptr) {
            if (ntp.ns != model::kafka_namespace) {
                // we only manage kafka namespace
                return;
            }
            auto it = _managed.find(ntp);
            if (it == _managed.end()) {
                // Leadership notification races with manage
                // notification?
                vlog(
                  _logger.warn,
                  "Unexpected leadership notification for partition {}",
                  ntp);
                return;
            }

            // Start archiving partition if needed. The node could be a
            // leader in a previous term. In this case we don't have to
            // do anything.
            vlog(
              _logger.info,
              "Archived partition {} received leadership (is_leader: "
              "{}) change notification in term {}.",
              ntp,
              is_leader,
              term);

            if (is_leader) {
                it->second->notify_leadership_acquired(term);
            } else {
                it->second->notify_leadership_lost();
            }
        }
    }

    fragmented_vector<model::ntp> managed_partitions() const {
        fragmented_vector<model::ntp> results;
        for (const auto& kv : _managed) {
            results.push_back(kv.first);
        }
        return results;
    }

    /// Snapshot of managed partitions which are leaders
    fragmented_vector<model::ntp> leader_partitions() const {
        fragmented_vector<model::ntp> results;
        for (const auto& kv : _managed) {
            if (kv.second->is_active()) {
                results.push_back(kv.first);
            }
        }
        return results;
    }

    model::node_id _self_node_id;
    ss::sharded<cluster::partition_manager>& _pm;
    ss::sharded<raft::group_manager>& _gm;
    ss::sharded<cloud_storage::remote>& _remote;
    ss::sharded<cloud_storage::cache>& _cache;
    ss::sharded<archival::upload_housekeeping_service>& _upload_housekeeping;
    ss::lw_shared_ptr<const configuration> _config;
    std::map<model::ntp, ss::shared_ptr<managed_partition>> _managed;
    std::optional<cluster::notification_id_type> _manage_notifications;
    std::optional<cluster::notification_id_type> _unmanage_notifications;
    std::optional<raft::group_manager_notification_id>
      _leadership_notifications;

    ss::shared_ptr<archiver_operations_api> _ops;
    ss::shared_ptr<archiver_scheduler_api<>> _sch;

    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _logger;
};

archiver_manager::archiver_manager(
  model::node_id node_id,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<raft::group_manager>& gm,
  ss::sharded<cloud_storage::remote>& api,
  ss::sharded<cloud_storage::cache>& cache,
  ss::sharded<archival::upload_housekeeping_service>& upload_housekeeping,
  ss::lw_shared_ptr<const configuration> config)
  : _impl(std::make_unique<impl>(
      node_id, pm, gm, api, cache, upload_housekeeping, config)) {}

archiver_manager::~archiver_manager() {}

ss::future<> archiver_manager::start() { co_await _impl->start(); }

ss::future<> archiver_manager::stop() { co_await _impl->stop(); }

fragmented_vector<model::ntp> archiver_manager::managed_partitions() const {
    return _impl->managed_partitions();
}

/// Snapshot of managed partitions which are leaders
fragmented_vector<model::ntp> archiver_manager::leader_partitions() const {
    return _impl->leader_partitions();
}

} // namespace archival
