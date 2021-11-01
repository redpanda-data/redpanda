/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "seastarx.h"
#include "utils/file_io.h"
#include "v8_engine/executor.h"
#include "v8_engine/script_dispatcher.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <chrono>
using namespace std::chrono_literals;

class env_for_test {
public:
    env_for_test()
      : _executor(ss::engine().alien(), 1, ss::smp::count)
      , _dispatcher(_executor) {
        _dispatcher.start().get();
    }

    ~env_for_test() {
        _dispatcher.stop().get();
        _executor.stop().get();
    }

    v8_engine::executor& get_executor() { return _executor; }
    v8_engine::script_dispatcher<v8_engine::executor>& get_dispather() {
        return _dispatcher;
    }

private:
    v8_engine::executor _executor;
    v8_engine::script_dispatcher<v8_engine::executor> _dispatcher;
};

v8_engine::enviroment env;

SEASTAR_THREAD_TEST_CASE(simple_update_test) {
    env_for_test env_wrapper;

    auto& dispatcher = env_wrapper.get_dispather();

    model::ns ns("test");
    model::topic topic("1");
    model::topic_namespace topic_ns(ns, topic);

    auto js_code = read_fully("to_upper.js").get();
    BOOST_REQUIRE(dispatcher.insert(topic_ns, "to_upper", std::move(js_code)));

    while (dispatcher.get(topic_ns).has_error()) {
        ss::sleep(10ms).get();
    }

    auto v8_runtime = dispatcher.get(topic_ns).value();

    ss::sstring raw_data = "qwerty";
    ss::temporary_buffer<char> data(raw_data.data(), raw_data.size());
    v8_runtime->run(data.share(), env_wrapper.get_executor()).get();
    boost::to_upper(raw_data);
    auto res = std::string(data.get_write(), data.size());
    BOOST_REQUIRE_EQUAL(raw_data, res);

    BOOST_REQUIRE(dispatcher.remove(topic_ns));
    auto get_res = dispatcher.get(topic_ns);
    BOOST_REQUIRE(get_res.has_error());
    BOOST_REQUIRE(get_res.error() == v8_engine::errc::v8_runtimes_not_exist);

    js_code = read_fully("to_upper.js").get();
    BOOST_REQUIRE(dispatcher.insert(topic_ns, "to_upper", std::move(js_code)));

    while (dispatcher.get(topic_ns).has_error()) {
        ss::sleep(10ms).get();
    }
    v8_runtime = dispatcher.get(topic_ns).value();

    ss::sstring raw_data2 = "qwerty";
    ss::temporary_buffer<char> data2(raw_data2.data(), raw_data2.size());
    v8_runtime->run(data2.share(), env_wrapper.get_executor()).get();
    boost::to_upper(raw_data2);
    res = std::string(data2.get_write(), data2.size());
    BOOST_REQUIRE_EQUAL(raw_data2, res);
}
