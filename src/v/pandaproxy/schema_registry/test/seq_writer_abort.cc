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

#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "ssx/abort_source.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <chrono>

namespace pps = pandaproxy::schema_registry;
using namespace std::chrono_literals;

namespace {

/// Transport whose read path parks, mimicking a broker whose kafka layer was
/// torn down mid-shutdown while a seq_writer operation is in flight.
///
/// The contract under test: seq_writer must hand its caller's abort_source
/// down to the transport, and a fired abort must resolve the parked wait
/// exceptionally. Without a handle the wait parks until the test's escape
/// hatch (release()), which models today's un-abortable hang.
class blocking_transport final : public noop_transport {
public:
    ss::future<model::offset> get_high_watermark(
      std::optional<std::reference_wrapper<ss::abort_source>> as) override {
        if (!_entered_set) {
            _entered_set = true;
            _entered.set_value();
        }
        if (as.has_value()) {
            try {
                co_await ss::sleep_abortable(1h, as->get());
            } catch (const ss::sleep_aborted&) {
                // Surface the abort itself (shutdown-classified), as the
                // production transport does via as.check().
                as->get().check();
            }
            co_return model::offset{0};
        }
        // No abort handle: park until the teardown escape hatch.
        co_await _release.get_future();
        co_return model::offset{0};
    }

    ss::future<> entered() { return _entered.get_future(); }

    void release() {
        if (!_released) {
            _released = true;
            _release.set_value();
        }
    }

private:
    bool _entered_set{false};
    bool _released{false};
    ss::promise<> _entered;
    ss::promise<> _release;
};

} // namespace

/// A seq_writer write parked in its transport read must resolve promptly and
/// shutdown-classified when the caller's abort_source fires. Guards the
/// cluster_link SR-sync shutdown path: task::stop() cannot drain its runner
/// while a destination write sits in an un-abortable transport wait.
SEASTAR_THREAD_TEST_CASE(seq_writer_write_aborts_parked_transport_wait) {
    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    blocking_transport transport;

    ss::sharded<pps::seq_writer> seq;
    seq
      .start(
        model::node_id{0},
        ss::default_smp_service_group(),
        std::ref(transport),
        std::reference_wrapper(s),
        ss::sharded_parameter(
          [] { return std::make_unique<sequence_state_checker_test>(); }))
      .get();
    auto stop_seq = ss::defer([&seq]() { seq.stop().get(); });
    // Runs before stop_seq: unwedge a still-parked write (the red state of
    // this test) so teardown does not hang.
    auto release_transport = ss::defer([&transport]() { transport.release(); });

    ss::abort_source parent;
    ssx::sharded_abort_source sas;
    sas.start(parent).get();
    auto stop_sas = ss::defer([&sas]() { sas.stop().get(); });

    auto fut = seq.local().write_config(
      pps::context_subject::unqualified("abort-test"),
      pps::compatibility_level::backward,
      pps::write_source::schema_registry_sync,
      &sas);

    // The write is parked inside the transport's read (read_sync).
    transport.entered().get();

    parent.request_abort();

    // Bounded on the assertion side only (no abandonment: the future is
    // polled, never dropped): the write must resolve exceptionally and
    // shutdown-classified well within the bound once the abort fires.
    for (int i = 0; i < 100 && !fut.available(); ++i) {
        ss::sleep(100ms).get();
    }
    BOOST_REQUIRE_MESSAGE(
      fut.available(),
      "write did not resolve within 10s of the abort; the abort_source is "
      "not reaching the parked transport wait");
    BOOST_REQUIRE(fut.failed());
    auto ex = fut.get_exception();
    BOOST_REQUIRE_MESSAGE(
      ssx::is_shutdown_exception(ex),
      "abort must surface as a shutdown-classified exception");
}
