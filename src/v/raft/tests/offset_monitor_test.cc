// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "raft/offset_monitor.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(wait) {
    raft::offset_monitor mon;

    auto f0 = mon.wait(model::offset(0), model::no_timeout, std::nullopt);
    auto f1 = mon.wait(model::offset(1), model::no_timeout, std::nullopt);
    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!f1.available());
    BOOST_REQUIRE(!mon.empty());

    mon.notify(model::offset(0));
    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(!f1.available());
    BOOST_REQUIRE_NO_THROW(f0.get());
    BOOST_REQUIRE(!mon.empty());

    mon.notify(model::offset(10));
    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE_NO_THROW(f1.get());
    BOOST_REQUIRE(mon.empty());
}

SEASTAR_THREAD_TEST_CASE(wait_multi) {
    raft::offset_monitor mon;

    auto f0_1 = mon.wait(model::offset(0), model::no_timeout, std::nullopt);
    auto f0_2 = mon.wait(model::offset(0), model::no_timeout, std::nullopt);
    auto f1 = mon.wait(model::offset(1), model::no_timeout, std::nullopt);
    BOOST_REQUIRE(!f0_1.available());
    BOOST_REQUIRE(!f0_2.available());
    BOOST_REQUIRE(!f1.available());
    BOOST_REQUIRE(!mon.empty());

    mon.notify(model::offset(0));
    BOOST_REQUIRE(f0_1.available());
    BOOST_REQUIRE(f0_2.available());
    BOOST_REQUIRE_NO_THROW(f0_1.get());
    BOOST_REQUIRE_NO_THROW(f0_2.get());
    BOOST_REQUIRE(!f1.available());
    BOOST_REQUIRE(!mon.empty());

    mon.notify(model::offset(10));
    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE_NO_THROW(f1.get());
    BOOST_REQUIRE(mon.empty());
}

SEASTAR_THREAD_TEST_CASE(stop) {
    raft::offset_monitor mon;
    ss::abort_source as;

    std::vector<ss::future<>> fs;

    fs.push_back(mon.wait(model::offset(0), model::no_timeout, std::nullopt));
    fs.push_back(mon.wait(model::offset(0), model::no_timeout, std::nullopt));
    fs.push_back(mon.wait(model::offset(10), model::no_timeout, std::nullopt));
    fs.push_back(mon.wait(model::offset(10), model::no_timeout, std::nullopt));

    fs.push_back(mon.wait(
      model::offset(0),
      model::timeout_clock::now() + std::chrono::seconds(30),
      std::nullopt));
    fs.push_back(mon.wait(
      model::offset(0),
      model::timeout_clock::now() + std::chrono::seconds(30),
      std::nullopt));
    fs.push_back(mon.wait(
      model::offset(10),
      model::timeout_clock::now() + std::chrono::seconds(30),
      std::nullopt));

    fs.push_back(mon.wait(model::offset(0), model::no_timeout, as));
    fs.push_back(mon.wait(model::offset(0), model::no_timeout, as));
    fs.push_back(mon.wait(model::offset(10), model::no_timeout, as));

    for (auto& f : fs) {
        BOOST_REQUIRE(!f.available());
    }

    BOOST_REQUIRE(!mon.empty());
    mon.stop();
    BOOST_REQUIRE(mon.empty());

    for (auto& f : fs) {
        BOOST_CHECK_THROW(f.get(), ss::abort_requested_exception);
    }
}

SEASTAR_THREAD_TEST_CASE(notify_abort) {
    raft::offset_monitor mon;
    ss::abort_source as;

    std::vector<ss::future<>> fs;

    fs.push_back(mon.wait(model::offset(0), model::no_timeout, std::nullopt));
    fs.push_back(mon.wait(model::offset(0), model::no_timeout, std::nullopt));
    fs.push_back(mon.wait(model::offset(10), model::no_timeout, std::nullopt));
    fs.push_back(mon.wait(model::offset(10), model::no_timeout, std::nullopt));

    fs.push_back(mon.wait(
      model::offset(0),
      model::timeout_clock::now() + std::chrono::seconds(30),
      std::nullopt));
    fs.push_back(mon.wait(
      model::offset(0),
      model::timeout_clock::now() + std::chrono::seconds(30),
      std::nullopt));
    fs.push_back(mon.wait(
      model::offset(10),
      model::timeout_clock::now() + std::chrono::seconds(30),
      std::nullopt));

    fs.push_back(mon.wait(model::offset(0), model::no_timeout, as));
    fs.push_back(mon.wait(model::offset(0), model::no_timeout, as));
    fs.push_back(mon.wait(model::offset(10), model::no_timeout, as));

    for (auto& f : fs) {
        BOOST_REQUIRE(!f.available());
    }

    BOOST_REQUIRE(!mon.empty());
    mon.notify(model::offset(100));
    BOOST_REQUIRE(mon.empty());

    for (auto& f : fs) {
        BOOST_CHECK_NO_THROW(f.get());
    }
}

SEASTAR_THREAD_TEST_CASE(wait_timeout) {
    raft::offset_monitor mon;

    auto f0 = mon.wait(
      model::offset(0), model::timeout_clock::now(), std::nullopt);
    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!mon.empty());

    BOOST_CHECK_THROW(f0.get(), ss::timed_out_error);

    BOOST_REQUIRE(mon.empty());
}

SEASTAR_THREAD_TEST_CASE(wait_abort_source) {
    raft::offset_monitor mon;
    ss::abort_source as;

    auto f0 = mon.wait(model::offset(0), model::no_timeout, as);
    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!mon.empty());

    as.request_abort();
    BOOST_REQUIRE(f0.failed());

    BOOST_CHECK_THROW(f0.get(), ss::abort_requested_exception);

    BOOST_REQUIRE(mon.empty());
}

SEASTAR_THREAD_TEST_CASE(wait_abort_source_already_aborted) {
    raft::offset_monitor mon;
    ss::abort_source as;

    as.request_abort();
    auto f0 = mon.wait(model::offset(0), model::no_timeout, as);
    BOOST_REQUIRE(mon.empty());

    BOOST_REQUIRE(f0.failed());
    BOOST_CHECK_THROW(f0.get(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(wait_abort_source_with_timeout_first) {
    raft::offset_monitor mon;
    ss::abort_source as;

    auto f0 = mon.wait(model::offset(0), model::timeout_clock::now(), as);
    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!mon.empty());

    // there is an abort source, but only wait on timeout

    BOOST_CHECK_THROW(f0.get(), ss::timed_out_error);

    BOOST_REQUIRE(mon.empty());
}

SEASTAR_THREAD_TEST_CASE(wait_abort_source_with_timeout_abort_first) {
    raft::offset_monitor mon;
    ss::abort_source as;

    as.request_abort();
    auto f0 = mon.wait(
      model::offset(0),
      model::timeout_clock::now() + std::chrono::seconds(2),
      as);
    BOOST_REQUIRE(f0.failed());
    BOOST_REQUIRE(mon.empty());

    BOOST_CHECK_THROW(f0.get(), ss::abort_requested_exception);

    BOOST_REQUIRE(mon.empty());
}

SEASTAR_THREAD_TEST_CASE(wait_abort_source_with_timeout_abort_before_timeout) {
    raft::offset_monitor mon;
    ss::abort_source as;

    auto f0 = mon.wait(
      model::offset(0),
      model::timeout_clock::now() + std::chrono::seconds(2),
      as);
    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!mon.empty());

    as.request_abort();

    BOOST_CHECK_THROW(f0.get(), ss::abort_requested_exception);

    BOOST_REQUIRE(mon.empty());
}
