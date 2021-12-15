/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/errc.h"
#include "hashing/xx.h"
#include "model/metadata.h"
#include "utils/file_io.h"
#include "v8_engine/api.h"
#include "v8_engine/data_policy.h"
#include "v8_engine/environment.h"
#include "v8_engine/executor.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/algorithm/string/case_conv.hpp>

class executor_wrapper_for_test {
public:
    executor_wrapper_for_test() {
        _executor.start(ss::engine().alien(), ss::smp::count).get();
    }

    ~executor_wrapper_for_test() { _executor.stop().get(); }

    v8_engine::executor_service& get_executor() { return _executor; }

private:
    v8_engine::executor_service _executor;
};

v8_engine::enviroment env;

void insert_code(
  std::string_view js_file, std::string_view code_name, v8_engine::api& api) {
    auto js_code = read_fully(js_file).get();
    size_t id(xxhash_64(code_name.data(), code_name.size()));
    api.insert_code(coproc::script_id(id), std::move(js_code)).get();
}

SEASTAR_THREAD_TEST_CASE(simple_test) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::api api(executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    v8_engine::data_policy new_dp("to_upper", "to_upper");
    insert_code("to_upper.js", new_dp.script_name(), api);
    BOOST_REQUIRE_EQUAL(
      api.insert_data_policy(topic, new_dp),
      std::error_code(cluster::errc::success));

    auto script = api.get_script(topic).get();
    BOOST_REQUIRE(script.get() != nullptr);

    ss::sstring raw_data = "qwerty";
    ss::temporary_buffer<char> data(raw_data.data(), raw_data.size());
    script->run(data.share(), executor_wrapper.get_executor()).get();
    boost::to_upper(raw_data);
    auto res = std::string(data.get_write(), data.size());
    BOOST_REQUIRE_EQUAL(raw_data, res);

    BOOST_REQUIRE_EQUAL(
      api.remove_data_policy(topic), std::error_code(cluster::errc::success));
    script = api.get_script(topic).get();
    BOOST_REQUIRE(script.get() == nullptr);
}

SEASTAR_THREAD_TEST_CASE(remove_inside_compilation) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::api api(executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    v8_engine::data_policy new_dp("foo", "long_compilation");
    insert_code("long_compilation.js", new_dp.script_name(), api);

    BOOST_REQUIRE_EQUAL(
      api.insert_data_policy(topic, new_dp),
      std::error_code(cluster::errc::success));
    auto get_script_future = api.get_script(topic);
    BOOST_REQUIRE_EQUAL(
      api.remove_data_policy(topic), std::error_code(cluster::errc::success));
    BOOST_REQUIRE(get_script_future.get().get() == nullptr);
}

SEASTAR_THREAD_TEST_CASE(error_in_compilation) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::api api(executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    v8_engine::data_policy new_dp("foo", "compile_timeout");
    insert_code("compile_timeout.js", new_dp.script_name(), api);

    BOOST_REQUIRE_EQUAL(
      api.insert_data_policy(topic, new_dp),
      std::error_code(cluster::errc::success));

    auto get_script_future = api.get_script(topic).get();
    BOOST_REQUIRE(get_script_future.get() == nullptr);
    // Should try to recompile script;
    get_script_future = api.get_script(topic).get();
    BOOST_REQUIRE(get_script_future.get() == nullptr);
}

SEASTAR_THREAD_TEST_CASE(check_waiting) {
    executor_wrapper_for_test executor_wrapper;
    v8_engine::api api(executor_wrapper.get_executor());

    model::topic_namespace topic(model::ns("test"), model::topic("simple"));

    v8_engine::data_policy new_dp("foo", "long_compilation");
    insert_code("long_compilation.js", new_dp.script_name(), api);

    BOOST_REQUIRE_EQUAL(
      api.insert_data_policy(topic, new_dp),
      std::error_code(cluster::errc::success));

    std::vector<ss::future<ss::lw_shared_ptr<v8_engine::script>>> futures;
    futures.reserve(10);
    for (int i = 0; i < 10; ++i) {
        futures.emplace_back(api.get_script(topic));
    }

    ss::when_all_succeed(futures.begin(), futures.end())
      .then([](std::vector<ss::lw_shared_ptr<v8_engine::script>> res) {
          for (auto ptr : res) {
              BOOST_REQUIRE(ptr.get() != nullptr);
          }
      })
      .get();
    BOOST_REQUIRE_EQUAL(
      api.remove_data_policy(topic), std::error_code(cluster::errc::success));

    BOOST_REQUIRE_EQUAL(
      api.insert_data_policy(topic, new_dp),
      std::error_code(cluster::errc::success));

    futures.clear();
    futures.reserve(10);
    for (int i = 0; i < 10; ++i) {
        futures.emplace_back(api.get_script(topic));
    }
    BOOST_REQUIRE_EQUAL(
      api.remove_data_policy(topic), std::error_code(cluster::errc::success));

    ss::when_all_succeed(futures.begin(), futures.end())
      .then([](std::vector<ss::lw_shared_ptr<v8_engine::script>> res) {
          for (auto ptr : res) {
              BOOST_REQUIRE(ptr.get() == nullptr);
          }
      })
      .get();
}
