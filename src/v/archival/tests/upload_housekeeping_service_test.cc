/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/fwd.h"
#include "archival/logger.h"
#include "archival/types.h"
#include "archival/upload_housekeeping_service.h"
#include "utils/retry_chain_node.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <chrono>

inline ss::logger test_log("test");
ss::abort_source abort_never;
class mock_job : public archival::housekeeping_job {
public:
    explicit mock_job(std::chrono::milliseconds ms)
      : _delay(ms) {}

    mock_job()
      : _delay(100ms)
      , _throw(true) {}

    ss::future<archival::housekeeping_job::run_result>
    run(retry_chain_node& rtc, archival::run_quota_t quota) override {
        ss::gate::holder h(_gate);
        if (_throw) {
            throw std::runtime_error("Job failed");
        }
        run_result result{
          .status = run_status::skipped,
          .consumed = archival::run_quota_t(0),
          .remaining = quota,
        };
        vlog(test_log.info, "mock job started");
        executed++;
        try {
            co_await ss::sleep_abortable(_delay, _as);
        } catch (ss::abort_requested_exception&) {
            vlog(test_log.info, "mock job sleep interrupted");
        }
        vlog(test_log.info, "mock job exited");
        co_return result;
    }
    void interrupt() override {
        BOOST_REQUIRE(interrupt_cnt == 0);
        vlog(test_log.info, "interrupt mock job");
        interrupt_cnt++;
        _as.request_abort();
    }

    bool interrupted() const override { return interrupt_cnt; }

    void set_enabled(bool) override {}

    ss::future<> stop() override { return _gate.close(); }

    void acquire() override {
        ss::gate::holder holder(_gate);
        _holder = std::move(holder);
    }

    void release() override { _holder.release(); }

    size_t executed{0};
    size_t interrupt_cnt{0};

private:
    std::chrono::milliseconds _delay;
    ss::abort_source _as;
    ss::gate _gate;
    ss::gate::holder _holder;
    bool _throw{false};
};

void wait_for_workflow_state(
  archival::housekeeping_workflow& wf,
  archival::housekeeping_state st,
  ss::lowres_clock::duration timeout = 10s) {
    vlog(test_log.debug, "Start waiting for the state {}", st);
    auto deadline = ss::lowres_clock::now() + timeout;
    while (wf.state() != st) {
        ss::sleep(1ms).get();
        if (ss::lowres_clock::now() > deadline) {
            break;
        }
    }
    vlog(test_log.debug, "Done waiting for the state {}", st);
    BOOST_REQUIRE(wf.state() == st);
}

void wait_for_job_execution(
  archival::housekeeping_workflow& wf,
  ss::lowres_clock::duration timeout = 10s) {
    vlog(test_log.debug, "Start waiting for the job");
    auto deadline = ss::lowres_clock::now() + timeout;
    while (!wf.has_active_job()) {
        ss::sleep(1ms).get();
        if (ss::lowres_clock::now() > deadline) {
            break;
        }
    }
    vlog(test_log.debug, "Done waiting for the job");
    BOOST_REQUIRE(wf.has_active_job());
}

SEASTAR_THREAD_TEST_CASE(test_housekeeping_workflow_stop) {
    retry_chain_node rtc(abort_never);
    archival::housekeeping_workflow wf(rtc);
    mock_job job1(10s);
    mock_job job2(10s);
    wf.register_job(job1);
    wf.register_job(job2);
    wf.start();
    wf.resume(false);
    wait_for_job_execution(wf);
    wf.deregister_job(job1);
    wf.deregister_job(job2);
    wf.stop().get();
    BOOST_REQUIRE_EQUAL(job1.executed, 1);
    BOOST_REQUIRE(job1.interrupted());
    BOOST_REQUIRE_EQUAL(job2.executed, 0);
    BOOST_REQUIRE(job2.interrupted());
    job1.stop().get();
    job2.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_housekeeping_workflow_pause) {
    retry_chain_node rtc(abort_never);
    archival::housekeeping_workflow wf(rtc);
    mock_job job1(10ms);
    mock_job job2(10ms);
    wf.register_job(job1);
    wf.register_job(job2);
    wf.start();
    wf.resume(false);
    wait_for_job_execution(wf);
    wf.pause();
    BOOST_REQUIRE_EQUAL(wf.state(), archival::housekeeping_state::pause);
    BOOST_REQUIRE_EQUAL(job1.executed, 1);
    BOOST_REQUIRE(!job1.interrupted());
    BOOST_REQUIRE_EQUAL(job2.executed, 0);
    BOOST_REQUIRE(!job2.interrupted());
    wf.resume(false);
    wait_for_workflow_state(wf, archival::housekeeping_state::idle);
    BOOST_REQUIRE_EQUAL(job1.executed, 1);
    BOOST_REQUIRE(!job1.interrupted());
    BOOST_REQUIRE_EQUAL(job2.executed, 1);
    BOOST_REQUIRE(!job2.interrupted());
    wf.deregister_job(job1);
    wf.deregister_job(job2);
    wf.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_housekeeping_workflow_drain) {
    retry_chain_node rtc(abort_never);
    archival::housekeeping_workflow wf(rtc);
    mock_job job1(10ms);
    mock_job job2(10ms);
    mock_job job3(10ms);
    mock_job job4(10ms);
    wf.register_job(job1);
    wf.register_job(job2);
    wf.register_job(job3);
    wf.register_job(job4);
    wf.start();
    wf.resume(true);
    wait_for_job_execution(wf);
    wf.pause(); // Should not stop
    wait_for_workflow_state(wf, archival::housekeeping_state::idle);
    BOOST_REQUIRE_EQUAL(job1.executed, 1);
    BOOST_REQUIRE(!job1.interrupted());
    BOOST_REQUIRE_EQUAL(job2.executed, 1);
    BOOST_REQUIRE(!job2.interrupted());
    BOOST_REQUIRE_EQUAL(job3.executed, 1);
    BOOST_REQUIRE(!job3.interrupted());
    BOOST_REQUIRE_EQUAL(job4.executed, 1);
    BOOST_REQUIRE(!job4.interrupted());
    wf.deregister_job(job1);
    wf.deregister_job(job2);
    wf.deregister_job(job3);
    wf.deregister_job(job4);
    wf.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_housekeeping_workflow_interrupt) {
    retry_chain_node rtc(abort_never);
    archival::housekeeping_workflow wf(rtc);
    mock_job job1(10s);
    mock_job job2(10ms);
    wf.register_job(job1);
    wf.register_job(job2);
    wf.start();
    wf.resume(false);
    wait_for_job_execution(wf);
    wf.deregister_job(job1);
    BOOST_REQUIRE_EQUAL(job1.executed, 1);
    BOOST_REQUIRE(job1.interrupted());
    BOOST_REQUIRE_EQUAL(job2.executed, 0);
    BOOST_REQUIRE(!job2.interrupted());
    wf.deregister_job(job2);
    wf.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_housekeeping_workflow_no_jobs) {
    retry_chain_node rtc(abort_never);
    archival::housekeeping_workflow wf(rtc);
    {
        mock_job job1(10s);
        mock_job job2(10ms);
        wf.register_job(job1);
        wf.register_job(job2);
        wf.start();
        wf.resume(false);
        wait_for_job_execution(wf);
        wf.deregister_job(job1);
        wf.deregister_job(job2);
        vlog(test_log.info, "both jobs deregistered");
        BOOST_REQUIRE_EQUAL(job1.executed, 1);
        BOOST_REQUIRE_EQUAL(job2.executed, 0);
        BOOST_REQUIRE(job1.interrupted());
        BOOST_REQUIRE(job2.interrupted());
        job1.stop().get();
        job2.stop().get();
        vlog(test_log.info, "both jobs stopped");
    }
    wf.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_housekeeping_workflow_job_throws) {
    retry_chain_node rtc(abort_never);
    archival::housekeeping_workflow wf(rtc);
    {
        mock_job job1; // This job will throw
        mock_job job2(10s);
        wf.register_job(job1);
        wf.register_job(job2);
        wf.start();
        wf.resume(false);
        wait_for_job_execution(wf);
        wf.deregister_job(job1);
        wf.deregister_job(job2);
        BOOST_REQUIRE_EQUAL(job1.executed, 0);
        BOOST_REQUIRE_EQUAL(job2.executed, 1);
        BOOST_REQUIRE(job1.interrupted());
        BOOST_REQUIRE(job2.interrupted());
        job1.stop().get();
        job2.stop().get();
    }
    wf.stop().get();
}
