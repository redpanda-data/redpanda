/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "model/metadata.h"
#include "utils/file_io.h"
#include "v8_engine/environment.h"
#include "v8_engine/executor.h"
#include "v8_engine/script_dispatcher.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string/case_conv.hpp>

class executor_wrapper_for_test {
public:
    executor_wrapper_for_test()
      : _executor(ss::engine().alien(), 1, ss::smp::count) {}

    ~executor_wrapper_for_test() { _executor.stop().get(); }

    v8_engine::executor& get_executor() { return _executor; }

private:
    v8_engine::executor _executor;
};

v8_engine::enviroment env;

SEASTAR_THREAD_TEST_CASE(simple_test) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::script_dispatcher<v8_engine::executor> sd(
      executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    auto js_code = read_fully("to_upper.js").get();
    BOOST_REQUIRE(sd.insert(topic, "to_upper", std::move(js_code)));
    auto script = sd.get(topic).get();
    BOOST_REQUIRE(script.get() != nullptr);

    ss::sstring raw_data = "qwerty";
    ss::temporary_buffer<char> data(raw_data.data(), raw_data.size());
    script->run(data.share(), executor_wrapper.get_executor()).get();
    boost::to_upper(raw_data);
    auto res = std::string(data.get_write(), data.size());
    BOOST_REQUIRE_EQUAL(raw_data, res);

    BOOST_REQUIRE(sd.remove(topic));
    script = sd.get(topic).get();
    BOOST_REQUIRE(script.get() == nullptr);
}

SEASTAR_THREAD_TEST_CASE(remove_inside_compilation) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::script_dispatcher<v8_engine::executor> sd(
      executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    auto js_code = read_fully("long_compilation.js").get();
    BOOST_REQUIRE(sd.insert(topic, "foo", std::move(js_code)));
    auto get_script_future = sd.get(topic);
    BOOST_REQUIRE(sd.remove(topic));
    BOOST_REQUIRE(get_script_future.get().get() == nullptr);
}

SEASTAR_THREAD_TEST_CASE(error_in_compilation) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::script_dispatcher<v8_engine::executor> sd(
      executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    auto js_code = read_fully("compile_timeout.js").get();
    BOOST_REQUIRE(sd.insert(topic, "foo", std::move(js_code)));
    auto get_script_future = sd.get(topic).get();
    BOOST_REQUIRE(get_script_future.get() == nullptr);
    // Should try to recompile script;
    get_script_future = sd.get(topic).get();
    BOOST_REQUIRE(get_script_future.get() == nullptr);
}

SEASTAR_THREAD_TEST_CASE(check_waiting) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::script_dispatcher<v8_engine::executor> sd(
      executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    auto js_code = read_fully("long_compilation.js").get();
    BOOST_REQUIRE(sd.insert(topic, "foo", std::move(js_code)));
    std::vector<ss::future<ss::lw_shared_ptr<v8_engine::script>>> futures;
    futures.reserve(10);
    for (int i = 0; i < 10; ++i) {
        futures.emplace_back(sd.get(topic));
    }

    ss::when_all_succeed(futures.begin(), futures.end())
      .then([](std::vector<ss::lw_shared_ptr<v8_engine::script>> res) {
          for (auto ptr : res) {
              BOOST_REQUIRE(ptr.get() != nullptr);
          }
      })
      .get();

    BOOST_REQUIRE(sd.remove(topic));

    js_code = read_fully("long_compilation.js").get();
    BOOST_REQUIRE(sd.insert(topic, "foo", std::move(js_code)));
    futures.clear();
    futures.reserve(10);
    for (int i = 0; i < 10; ++i) {
        futures.emplace_back(sd.get(topic));
    }
    BOOST_REQUIRE(sd.remove(topic));

    ss::when_all_succeed(futures.begin(), futures.end())
      .then([](std::vector<ss::lw_shared_ptr<v8_engine::script>> res) {
          for (auto ptr : res) {
              BOOST_REQUIRE(ptr.get() == nullptr);
          }
      })
      .get();
}
