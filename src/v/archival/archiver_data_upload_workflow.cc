/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archiver_data_upload_workflow.h"

#include "archival/archiver_operations_api.h"
#include "archival/archiver_scheduler_api.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "config/configuration.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/loop.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <boost/msm/back/state_machine.hpp>
#include <boost/msm/front/euml/common.hpp>
#include <boost/msm/front/functor_row.hpp>
#include <boost/msm/front/state_machine_def.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

#include <chrono>
#include <exception>
#include <memory>
#include <system_error>
#include <tuple>

namespace archival {

////////////////////////////////////

/* clang-format off

Data upload FSM
                             success
                   ┌────────────┐  
        ┌──────────┤ upload man ├───┐         
        │          └────────────┘   │         
        │           on   ▲          │         
        ▼           idle │          │         
 ┌────────────┐    ┌─────┴──────┐   │   ┌────────────┐       
 │ terminal   │◄───┤ backoff    │◄──┤───┤ initial    │       
 └────────────┘    └─────┬──────┘   │   └────────────┘       
        ▲                │          │         
 not a  │                ▼   no data│         
 leader │          ┌────────────┐   │         
 or     ├──────────┤ reconcile  ├───┤         
 fatal  │          └─────┬──────┘   │         
 error  │                │          │         
 or     │                ▼     error│         
 shut-  │          ┌────────────┐   │         
 down   ├──────────┤ upload     ├───┤         
        │          └─────┬──────┘   │         
        │                │          │         
        │                ▼   success│         
        │          ┌────────────┐   │         
        └──────────┤ admit      ├───┘         
                   └────────────┘             

The FSM is running a loop. It starts in _initial_ state.
Once the upload is completed the FSM transitions into the
_backoff_ state. When it shutdown the FSM transitions to
the _terminal_ state. In this state the FSM can be safely
disposed. It's guaranteed that there are no background fibers
when FSM is in the _terminal_ state.

The loop is implicit. There is no control flow in the traditional
sense. Instead the state transitions are repeated until the shutdown.

The states of the FSM are:
* _initial_ state is a beginning of the loop iteration. 
* _backoff_ state is an initial stop in the loop. This is the
  place where we can apply back-pressure or detect that the loop
  is idle. When the backoff period or throttling event is over
  the state can either transition to the _upload_manifest_ state or
  transition to the _reconcile_ state. If the term is over the FSM
  transitions to the _terminal_ state. This state interacts with
  the 'archiver_scheduler_api' instance.
* _upload_manifest_ performs manifest upload if the upload loop was
  idle long enough and the archival_stm state is 'dirty'.
* _reconcile_ state is looking up new data to upload in the local
  storage. If there is not enough data we should transition back to
  _backoff_ state. Otherwise the FSM transitions to the next state.
* _upload_ is a state in which we upload data to S3. The data includes
  the segment, its aux. structures (index, tx-manifest), and the
  partition manifest (for manifest upload pipelining).
* _admit_ state is responsible for adding upload metadata to the
  archival metadata STM.
* _terminal_ is a final state of the FSM in which it can be disposed.
  We transition to this state after the fatal error or shutdown of the
  upload loop or when the leadership is lost. We can shutdown while
  being a leader if the Redpanda is shutting down.

We can transition from any state except the _terminal_ to the 
_backoff_ state if recoverable error occurred.
We can transition from any state to the _terminal_ state on fatal
error, shutdown event or when the leadership is lost.

clang-format on */

enum class data_upload_workflow_states_t {
    // Initial state of the loop.
    initial,
    // Apply throttling
    backoff,
    // Find new data to upload (compacted or non-compacted)
    reconcile,
    // Upload the data to S3
    upload,
    // Manifest-only upload
    upload_manifest,
    // Admit upload metadata into the archival STM
    admit,
    // Terminal state
    terminal,
};

std::ostream& operator<<(std::ostream& o, data_upload_workflow_states_t t) {
    switch (t) {
        using enum data_upload_workflow_states_t;
    case initial:
        return o << "initial";
    case backoff:
        return o << "backoff";
    case reconcile:
        return o << "reconcile";
    case upload:
        return o << "upload";
    case upload_manifest:
        return o << "upload_manifest";
    case admit:
        return o << "admit";
    case terminal:
        return o << "terminal";
    }
}

struct data_workflow_configs {
    config::binding<size_t> local_segment_size;
    config::binding<std::optional<size_t>> target_segment_size;
    config::binding<std::optional<size_t>> min_segment_size;
    config::binding<std::chrono::milliseconds> segment_upload_timeout;
    config::binding<std::chrono::milliseconds> manifest_upload_timeout;
    config::binding<std::chrono::milliseconds> metadata_sync_timeout;
    config::binding<std::chrono::milliseconds> upload_backoff;
    config::binding<bool> compacted_reuploads_enabled;

    data_workflow_configs()
      : local_segment_size(config::shard_local_cfg().log_segment_size.bind())
      , target_segment_size(
          config::shard_local_cfg().cloud_storage_segment_size_target.bind())
      , min_segment_size(
          config::shard_local_cfg().cloud_storage_segment_size_min.bind())
      , segment_upload_timeout(
          config::shard_local_cfg()
            .cloud_storage_segment_upload_timeout_ms.bind())
      , manifest_upload_timeout(
          config::shard_local_cfg()
            .cloud_storage_manifest_upload_timeout_ms.bind())
      , metadata_sync_timeout(config::shard_local_cfg()
                                .cloud_storage_metadata_sync_timeout_ms.bind())
      , upload_backoff(
          config::shard_local_cfg().cloud_storage_initial_backoff_ms.bind())
      , compacted_reuploads_enabled(
          config::shard_local_cfg()
            .cloud_storage_enable_compacted_topic_reupload.bind()) {}
};

struct data_upload_workflow_fsm_base {};

// FSM that encodes data upload workflow operations
struct data_upload_workflow_fsm
  : public boost::msm::front::state_machine_def<
      data_upload_workflow_fsm,
      data_upload_workflow_fsm_base> {
public:
    using state_machine_t = boost::msm::back::
      state_machine<data_upload_workflow_fsm, data_upload_workflow_fsm_base>;

    template<data_upload_workflow_states_t id>
    struct state
      : boost::msm::front::state<data_upload_workflow_fsm_base>
    // have to derive from *_fsm_base because data_upload_workflow_fsm is
    // incomplete when this class is instantiated
    {
        std::optional<retry_chain_node> rtc;

        // Start async operation
        void on_start_state(
          std::chrono::milliseconds timeout,
          std::chrono::milliseconds backoff,
          state_machine_t& fsm) {
            rtc.emplace(timeout, backoff, &fsm._rtc);
        }

        template<class T>
        void on_entry(const T&, state_machine_t& fsm) {
            vlog(
              fsm._log.debug,
              "Enter state {}, rtc set {}",
              id,
              rtc.has_value());
        }

        template<class T>
        void on_exit(const T&, state_machine_t& fsm) {
            vlog(fsm._log.debug, "Exit state {}", id);
            rtc.reset();
        }
    };

    struct st_initial : state<data_upload_workflow_states_t::initial> {};
    struct st_backoff_async : state<data_upload_workflow_states_t::backoff> {};
    struct st_reconcile_async
      : state<data_upload_workflow_states_t::reconcile> {};
    struct st_upload_async : state<data_upload_workflow_states_t::upload> {
        // Set to true if the manifest has to be uploaded in
        // parallel with segments
        bool inline_manifest{false};
    };
    struct st_admit_async : state<data_upload_workflow_states_t::admit> {
        // Number of PUT requests used by previous upload step
        size_t num_put_request{0};
        // Number of bytes sent by previous upload step
        size_t num_bytes_sent{0};
    };
    struct st_manifest_async
      : state<data_upload_workflow_states_t::upload_manifest> {
        // This is a projected clean offset. The value is set after the manifest
        // is uploaded.
        std::optional<model::offset> manifest_clean_offset;
        // Dirty offset of the manifest. On success we should set this value to
        // the current in-sync offset of the manifest.
        std::optional<model::offset> manifest_dirty_offset;

        bool is_dirty() const {
            return manifest_dirty_offset.has_value()
                   && manifest_clean_offset != manifest_dirty_offset;
        }

        void set_dirty_offset(model::offset o) {
            manifest_dirty_offset = o != model::offset{} ? std::make_optional(o)
                                                         : std::nullopt;
        }

        void set_clean_offset(model::offset o) {
            manifest_clean_offset = o != model::offset{} ? std::make_optional(o)
                                                         : std::nullopt;
        }
    };
    struct st_terminal : state<data_upload_workflow_states_t::terminal> {
        // Optional error in case if shutdown is not clean
        std::exception_ptr err;
        // If this is set then 'stop' method is waiting for shutdown to happen
        ss::promise<> completed_shutdown;
    };

    // The ev_start is the only event triggered synchronously
    // outside of the FSM. It's triggered when the FSM starts and initiates
    // transition from initial to backoff state. All remaining events are
    // triggered by API methods. The pattern is that the transition function
    // starts a background operation associated with the next state (the state
    // to which the FSM transitions) during the state transition. When the
    // background operation finishes the event is generated depending of the
    // result of the background operation. So basically, every state except the
    // st_initial and st_terminal is associated with a single API call (either
    // from archiver_operations_api or archiver_scheduler_api). And when the FSM
    // is in this state it's guaranteed that the background operation is
    // in-progress.

    struct ev_start {};
    using ev_reconcile = archiver_operations_api::find_upload_candidates_arg;
    using ev_reupload_manifest = archiver_operations_api::manifest_upload_arg;
    using ev_manifest_done = archiver_operations_api::manifest_upload_result;
    using ev_upload_candidates
      = archiver_operations_api::find_upload_candidates_result;
    using ev_upload_results = archiver_operations_api::schedule_upload_results;
    using ev_admit_results = archiver_operations_api::admit_uploads_result;
    struct ev_nothing_to_upload {
        model::ktp ntp;
    };
    // Any recoverable error.
    struct ev_recoverable_error {
        std::error_code errc;
    };
    // Unrecoverable error.
    // The error_code should be converted to system_error.
    struct ev_fatal_error {
        std::exception_ptr err;
        bool shutdown{false};
    };

    // Every state transition starts a background fiber. When the fiber
    // becomes ready it invokes process_event which triggers next state
    // transition.

    struct tr_initial_to_backoff {
        void operator()(
          const ev_start&,
          state_machine_t& fsm,
          st_initial& prev [[maybe_unused]],
          st_backoff_async& next [[maybe_unused]]) {
            ssx::background = fsm.maybe_suspend(
              fsm,
              archiver_scheduler_api::suspend_upload_arg{
                .ntp = fsm._ntp,
                // Force manifest reupload in the beginning of the term
                .manifest_dirty = true,
              });
        }
    };

    struct tr_backoff_to_reconcile {
        void operator()(
          const ev_reconcile& ev,
          state_machine_t& fsm,
          st_backoff_async& prev [[maybe_unused]],
          st_reconcile_async& next [[maybe_unused]]) {
            auto reconcile_timeout = fsm._cfg.segment_upload_timeout();
            auto reconcile_backoff = fsm._cfg.upload_backoff();
            next.on_start_state(reconcile_timeout, reconcile_backoff, fsm);

            vassert(next.rtc.has_value(), "RTC is not set");
            fsm.get_state<st_upload_async&>().inline_manifest
              = ev.inline_manifest;
            ssx::background
              = fsm._ops_api->find_upload_candidates(next.rtc.value(), ev)
                  .then_wrapped(
                    [&fsm](ss::future<result<ev_upload_candidates>> fut) {
                        auto vec = fsm.handle_errors(std::move(fut), fsm);
                        if (!vec.has_value()) {
                            return;
                        }
                        if (vec->results.empty()) {
                            // Not enough data to start a new upload
                            fsm.process_event(ev_nothing_to_upload{});
                        } else {
                            // Propagate upload candidate forward
                            fsm.process_event(vec.value());
                        }
                    });
        }
    };

    struct tr_backoff_to_manifest {
        void operator()(
          const ev_reupload_manifest& ev,
          state_machine_t& fsm,
          st_backoff_async& prev [[maybe_unused]],
          st_manifest_async& next [[maybe_unused]]) {
            auto upload_timeout = fsm._cfg.manifest_upload_timeout();
            auto upload_backoff = fsm._cfg.upload_backoff();
            next.on_start_state(upload_timeout, upload_backoff, fsm);
            vassert(next.rtc.has_value(), "RTC is not set");
            ssx::background
              = fsm._ops_api->upload_manifest(next.rtc.value(), ev)
                  .then_wrapped(
                    [&fsm](ss::future<result<ev_manifest_done>> fut) {
                        auto res = fsm.handle_errors(std::move(fut), fsm);
                        if (!res.has_value()) {
                            return;
                        }
                        fsm.process_event(res.value());
                    });
        }
    };

    // Backoff triggered by the recoverable error
    struct tr_any_to_backoff {
        template<class Src>
        void operator()(
          const ev_recoverable_error& ev,
          state_machine_t& fsm,
          Src& prev [[maybe_unused]],
          st_backoff_async& next [[maybe_unused]]) {
            vlog(fsm._log.debug, "Recoverable error: {}", ev.errc);
            ssx::background = fsm.maybe_suspend(
              fsm,
              archiver_scheduler_api::suspend_upload_arg{
                .ntp = fsm._ntp,
                .errc = ev.errc,
              });
        }
    };

    struct tr_any_to_terminal {
        template<class Src>
        void operator()(
          const ev_fatal_error& ev,
          state_machine_t& fsm,
          Src&,
          st_terminal& next) {
            if (!ev.shutdown) {
                vlog(fsm._log.error, "Unrecoverable error: {}", ev.err);
                next.err = ev.err;
                next.completed_shutdown.set_exception(ev.err);
            } else {
                vlog(fsm._log.debug, "Shutdown error");
                next.completed_shutdown.set_value();
            }
        }
        template<class Src>
        void operator()(
          const ev_recoverable_error& ev,
          state_machine_t& fsm,
          Src&,
          st_terminal& next) {
            vlog(fsm._log.debug, "Recoverable error: {}", ev.errc);
            next.completed_shutdown.set_value();
        }
    };

    struct tr_reconcile_to_backoff {
        void operator()(
          const ev_nothing_to_upload& ev,
          state_machine_t& fsm,
          st_reconcile_async& prev [[maybe_unused]],
          st_backoff_async& next [[maybe_unused]]) {
            vlog(fsm._log.debug, "Not enough data to upload");
            ssx::background = fsm.maybe_suspend(
              fsm,
              archiver_scheduler_api::suspend_upload_arg{
                .ntp = ev.ntp,
                .errc = error_outcome::not_enough_data,
              });
        }
    };

    struct tr_reconcile_to_upload {
        void operator()(
          ev_upload_candidates ev,
          state_machine_t& fsm,
          st_reconcile_async& prev [[maybe_unused]],
          st_upload_async& next) {
            vlog(
              fsm._log.debug,
              "Going to upload {} segments, inline_manifest: {}",
              ev.results.size(),
              next.inline_manifest);
            if (archival_log.level() > ss::log_level::info) {
                for (const auto& c : ev.results) {
                    vlog(fsm._log.debug, "Upload candidate: {}", c);
                }
            }
            auto upload_timeout = fsm._cfg.segment_upload_timeout();
            auto upload_backoff = fsm._cfg.upload_backoff();
            next.on_start_state(upload_timeout, upload_backoff, fsm);
            vassert(next.rtc.has_value(), "RTC is not set");
            ssx::background
              = fsm._ops_api
                  ->schedule_uploads(
                    next.rtc.value(), std::move(ev), next.inline_manifest)
                  .then_wrapped(
                    [&fsm](ss::future<result<
                             archiver_operations_api::schedule_upload_results>>
                             fut) {
                        auto res = fsm.handle_errors(std::move(fut), fsm);
                        if (!res.has_value()) {
                            return;
                        }
                        vlog(
                          fsm._log.debug, "Uploads finished: {}", res.value());
                        // Uploads completed at least partially. Next step
                        // is to sort them out and admit changes to the
                        // archival STM.
                        fsm.process_event(std::move(res).value());
                    });
        }
    };

    struct tr_upload_to_admit {
        void operator()(
          ev_upload_results ev,
          state_machine_t& fsm,
          st_upload_async& prev [[maybe_unused]],
          st_admit_async& next) {
            vlog(
              fsm._log.debug,
              "Going to admit metadata of the uploaded segments into the "
              "archival STM {}",
              ev);

            next.num_bytes_sent = ev.num_bytes_sent;
            next.num_put_request = ev.num_put_requests;

            // Propagate the manifest clean offset to the manifest upload state
            // state.
            auto& manifest_upl_state = fsm.get_state<st_manifest_async&>();
            manifest_upl_state.set_clean_offset(ev.manifest_clean_offset);

            auto sync_timeout = fsm._cfg.metadata_sync_timeout();
            auto sync_backoff = fsm._cfg.upload_backoff();
            next.on_start_state(sync_timeout, sync_backoff, fsm);

            vassert(next.rtc.has_value(), "RTC is not set");
            ssx::background
              = fsm._ops_api->admit_uploads(next.rtc.value(), std::move(ev))
                  .then_wrapped(
                    [&fsm](
                      ss::future<result<
                        archiver_operations_api::admit_uploads_result>> fut) {
                        auto res = fsm.handle_errors(std::move(fut), fsm);
                        if (!res.has_value()) {
                            return;
                        }
                        vlog(
                          fsm._log.debug,
                          "Upload metadata admitted into the archival STM: {}",
                          res.value());
                        fsm.process_event(res.value());
                    });
        }
    };

    struct tr_admit_to_backoff {
        void operator()(
          const ev_admit_results& ev,
          state_machine_t& fsm,
          st_admit_async& prev,
          st_backoff_async& next [[maybe_unused]]) {
            auto& st = fsm.get_state<st_manifest_async&>();
            st.set_dirty_offset(ev.manifest_dirty_offset);
            ssx::background = fsm.maybe_suspend(
              fsm,
              archiver_scheduler_api::suspend_upload_arg{
                .ntp = ev.ntp,
                // The scheduler should know that the manifest is dirty
                // in order to be able to schedule the upload if the things
                // are idle long enough.
                .manifest_dirty = st.is_dirty(),
                // These values are propagated from the upload step
                // The invariant is that these values are always correct
                // when we get to this place.
                .put_requests_used = prev.num_put_request,
                .uploaded_bytes = prev.num_bytes_sent,
              });
        }
    };

    struct tr_manifest_to_backoff {
        void operator()(
          const ev_manifest_done& ev,
          state_machine_t& fsm,
          st_manifest_async& prev [[maybe_unused]],
          st_backoff_async& next [[maybe_unused]]) {
            auto& st = fsm.get_state<st_manifest_async&>();
            st.manifest_dirty_offset.reset();
            st.manifest_clean_offset.reset();
            ssx::background = fsm.maybe_suspend(
              fsm,
              archiver_scheduler_api::suspend_upload_arg{
                .ntp = ev.ntp,
                .manifest_dirty = false,
                .put_requests_used = ev.num_put_requests,
                .uploaded_bytes = ev.size_bytes,
              });
        }
    };

    template<class Src, class Event, class Target, class Action>
    using row = boost::msm::front::Row<Src, Event, Target, Action>;
    // clang-format off
    using transition_table = boost::mpl::vector<
    //    Initial state       Event                     Target state        Transition
      row<st_initial,         ev_start,                 st_backoff_async,   tr_initial_to_backoff>,

      row<st_backoff_async,   ev_reconcile,             st_reconcile_async, tr_backoff_to_reconcile>,
      row<st_backoff_async,   ev_fatal_error,           st_terminal,        tr_any_to_terminal>,
      row<st_backoff_async,   ev_reupload_manifest,     st_manifest_async,  tr_backoff_to_manifest>,

      row<st_reconcile_async, ev_recoverable_error,     st_backoff_async,   tr_any_to_backoff>,
      row<st_reconcile_async, ev_fatal_error,           st_terminal,        tr_any_to_terminal>,
      row<st_reconcile_async, ev_nothing_to_upload,     st_backoff_async,   tr_reconcile_to_backoff>,
      row<st_reconcile_async, ev_upload_candidates,     st_upload_async,    tr_reconcile_to_upload>,

      row<st_upload_async,    ev_recoverable_error,     st_backoff_async,   tr_any_to_backoff>,
      row<st_upload_async,    ev_fatal_error,           st_terminal,        tr_any_to_terminal>,
      row<st_upload_async,    ev_upload_results,        st_admit_async,     tr_upload_to_admit>,

      row<st_admit_async,     ev_recoverable_error,     st_backoff_async,   tr_any_to_backoff>,
      row<st_admit_async,     ev_fatal_error,           st_terminal,        tr_any_to_terminal>,
      row<st_admit_async,     ev_admit_results,         st_backoff_async,   tr_admit_to_backoff>,

      row<st_manifest_async,  ev_manifest_done,         st_backoff_async,   tr_manifest_to_backoff>,
      row<st_manifest_async,  ev_recoverable_error,     st_backoff_async,   tr_any_to_backoff>,
      row<st_manifest_async,  ev_fatal_error,           st_terminal,        tr_any_to_terminal>
    >;
    // clang-format on

    using initial_state = st_initial;

    template<class T>
    std::optional<T>
    handle_errors(ss::future<result<T>> fut, state_machine_t& fsm) {
        if (fut.failed()) {
            auto e = fut.get_exception();
            vlog(_log.debug, "Failed background future {}", e);
            if (ssx::is_shutdown_exception(e)) {
                // Trigger shutdown error
                fsm.process_event(ev_fatal_error{.shutdown = true});
            } else {
                // Unrecoverable error
                fsm.process_event(ev_fatal_error{.err = e});
            }
            return std::nullopt;
        }
        auto res = std::move(fut.get());
        if (res.has_error()) {
            vlog(
              _log.debug,
              "Background future returned error result: {}",
              res.error());
            if (
              res.error() == cloud_storage::error_outcome::shutting_down
              || res.error() == error_outcome::shutting_down) {
                // Shutdown error
                fsm.process_event(ev_fatal_error{.shutdown = true});
            } else {
                // Recoverable error
                fsm.process_event(ev_recoverable_error{.errc = res.error()});
            }
            return std::nullopt;
        }
        return std::move(res).value();
    }

    ev_reconcile get_reconcile_params(
      bool inline_manifest, ssize_t upload_size_quota, ssize_t requests_quota) {
        auto [low, high] = get_segment_size_bounds();
        ev_reconcile res{
          .ntp = _ntp,
          .target_size = high,
          .min_size = low,
          .upload_size_quota = upload_size_quota,
          .upload_requests_quota = requests_quota,
          .compacted_reupload = _cfg.compacted_reuploads_enabled(),
          .inline_manifest = inline_manifest,
        };
        return res;
    }

    ss::future<> maybe_suspend(
      state_machine_t& fsm, archiver_scheduler_api::suspend_upload_arg req) {
        vlog(_log.debug, "Workflow could be suspended {}", req);
        return fsm._scheduler->maybe_suspend_upload(req).then_wrapped(
          [&fsm](
            ss::future<result<archiver_scheduler_api::next_upload_action_hint>>
              fut) {
              auto opt = fsm.handle_errors(std::move(fut), fsm);
              if (!opt.has_value()) {
                  return;
              }
              vlog(fsm._log.debug, "Workflow resumed {}", opt.value());
              switch (opt->type) {
                  using enum archiver_scheduler_api::next_upload_action_type;
              case segment_upload:
                  fsm.process_event(fsm.get_reconcile_params(
                    false, opt->upload_size_quota, opt->requests_quota));
                  break;
              case segment_with_manifest:
                  fsm.process_event(fsm.get_reconcile_params(
                    true, opt->upload_size_quota, opt->requests_quota));
                  break;
              case manifest_upload:
                  fsm.process_event(ev_reupload_manifest{
                    .ntp = fsm._ntp,
                  });
                  break;
              };
          });
    }

    ss::future<> shutdown(state_machine_t& fsm) {
        vassert(!_as.abort_requested(), "shutdown is called twice");
        _as.request_abort();
        auto& st = fsm.get_state<st_terminal&>();
        vlog(_log.info, "Waiting for the workflow shutdown");
        return st.completed_shutdown.get_future();
    }

    explicit data_upload_workflow_fsm(
      ss::shared_ptr<archiver_operations_api> api,
      ss::shared_ptr<archiver_scheduler_api> quota_api,
      model::ktp ntp,
      model::term_id term)
      : _ops_api(std::move(api))
      , _scheduler(std::move(quota_api))
      , _ntp(std::move(ntp))
      , _term(term)
      , _rtc(_as)
      , _log(
          archival_log, _rtc, ssx::sformat("{}-{}", _ntp.to_ntp().path(), term))
      , _cfg{} {}

    // Default exception handler.
    template<class FSM, class Event>
    void exception_caught(const Event&, FSM& fsm, std::exception& err) {
        vlog(_log.error, "Unexpected exception {}", err);
        fsm.process_event(ev_fatal_error{.err = std::make_exception_ptr(err)});
    }

private:
    std::pair<size_t, size_t> get_segment_size_bounds() {
        auto high = _cfg.target_segment_size().value_or(
          _cfg.local_segment_size());
        auto low = _cfg.min_segment_size().value_or(high / 2);
        if (low > high) {
            low = high * 8 / 10;
        }
        return std::make_pair(low, high);
    }

    ss::shared_ptr<archiver_operations_api> _ops_api;
    ss::shared_ptr<archiver_scheduler_api> _scheduler;
    model::ktp _ntp;
    model::term_id _term;
    ss::abort_source _as;
    retry_chain_node _rtc;
    retry_chain_logger _log;
    data_workflow_configs _cfg;
};

class data_upload_workflow
  : public archiver_workflow_api
  , public data_upload_workflow_fsm::state_machine_t {
public:
    explicit data_upload_workflow(
      ss::shared_ptr<archiver_operations_api> api,
      ss::shared_ptr<archiver_scheduler_api> quota_api,
      model::ktp ntp,
      model::term_id id)
      : state_machine_t(
        std::move(api), std::move(quota_api), std::move(ntp), id) {}

    ss::future<> start() override {
        // This is not actually async. Keeping it this way to be
        // consistent with the rest of the codebase.
        process_event(ev_start{});
        co_return;
    }
    ss::future<> stop() override {
        co_await shutdown(*this);
        co_return;
    }
};

ss::shared_ptr<archiver_workflow_api> make_data_upload_workflow(
  model::ktp ntp,
  model::term_id id,
  ss::shared_ptr<archiver_operations_api> api,
  ss::shared_ptr<archiver_scheduler_api> sched) {
    vlog(
      archival_log.debug,
      "Creating data upload workflow for {} in term {}",
      ntp,
      id);
    return ss::make_shared<data_upload_workflow>(api, sched, ntp, id);
}

} // namespace archival
