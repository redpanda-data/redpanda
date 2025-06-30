/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "serde/json/tests/dom_serde.h"

#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>

namespace {

// Seastar runs in this thread, and the fuzzer submits work
// with the alien instance.
static std::thread seastar_thread_;
static seastar::alien::instance* alien_instance_ = nullptr;

// Seastar init.
static std::mutex init_mutex_;
static std::condition_variable init_cv_;
static bool init_complete_ = false;

// Seastar shutdown.
static std::mutex shutdown_mutex_;
static std::condition_variable shutdown_cv_;
static std::atomic<bool> shutdown_requested{false};
static bool shutdown_complete_ = false;

static void initialize_seastar() {
    seastar_thread_ = std::thread([]() {
        seastar::app_template::seastar_options seastar_config;
        seastar_config.smp_opts.smp.set_value(1);
        seastar_config.reactor_opts.overprovisioned.set_value();

        // It's possible but not recommended to pass in
        // the command line through the fuzzer.
        seastar::app_template app(std::move(seastar_config));
        seastar::sstring program_name = "json_fuzz";
        std::array<char*, 1> args = {program_name.data()};
        app.run(args.size(), args.data(), []() -> seastar::future<int> {
            alien_instance_ = &seastar::engine().alien();

            {
                std::lock_guard<std::mutex> lock(init_mutex_);
                init_complete_ = true;
            }
            init_cv_.notify_one();

            while (!shutdown_requested.load()) {
                co_await seastar::sleep(std::chrono::milliseconds(100));
            }

            {
                std::lock_guard<std::mutex> lock(shutdown_mutex_);
                shutdown_complete_ = true;
            }
            shutdown_cv_.notify_one();

            co_return 0;
        });
    });

    {
        std::unique_lock<std::mutex> lock(init_mutex_);
        init_cv_.wait(lock, []() { return init_complete_; });
    }

    // Handler to clean up seastar nicely.
    std::atexit([]() {
        shutdown_requested.store(true);
        {
            std::unique_lock<std::mutex> lock(shutdown_mutex_);
            shutdown_cv_.wait(lock, []() { return shutdown_complete_; });
        }
        seastar_thread_.join();
    });
}

seastar::future<> parse_json_async(const uint8_t* data, size_t size) {
    iobuf buf;
    buf.append(data, size);

    try {
        auto result
          = co_await experimental::serde::json::test::dom::parse_document_serde(
            std::move(buf));
        (void)result;
    } catch (...) {
        // Parse errors are expected.
    }
    co_return;
}

static void parse_json(const uint8_t* data, size_t size) {
    return seastar::alien::submit_to(
             *alien_instance_,
             0,
             [data, size]() { return parse_json_async(data, size); })
      .get();
}

} // anonymous namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    static bool initialized = false;
    if (!initialized) {
        initialize_seastar();
        initialized = true;
    }

    parse_json(data, size);
    return 0;
}
