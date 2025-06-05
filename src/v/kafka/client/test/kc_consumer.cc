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

#include "base/seastarx.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/client/manual_consumer.h"
#include "model/fundamental.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/smp_options.hh>

#include <boost/program_options/errors.hpp>

#include <chrono>
#include <exception>
#include <functional>
#include <numeric>

using namespace std::chrono_literals;

struct app_configs {
    std::string topic{};
    size_t n_partitions{};
    std::chrono::milliseconds consumer_max_wait_ms;
    std::chrono::milliseconds fetch_sleep_ms;
};

ss::future<> run(const app_configs& app_conf) {
    fmt::print("Running...\n");
    auto configs = kafka::client::configuration{};
    configs.consumer_request_timeout.set_value(1s);

    auto client = kafka::client::client(
      config::to_yaml(configs, config::redact_secrets::no));
    auto topics = kafka::client::manual_consumer::cursors{};
    auto topic = model::topic{app_conf.topic};
    auto& tp_cursors = topics[topic];
    for (size_t i = 0; i < app_conf.n_partitions; ++i) {
        tp_cursors[model::partition_id{static_cast<int>(i)}]
          = kafka::client::manual_consumer::cursor{model::offset{0}};
    }

    auto consumer = client.create_manual_consumer(std::move(topics));
    auto total_count = size_t{0};

    while (true) {
        try {
            auto res = co_await consumer->fetch();
            fmt::print("Fetch response: {}\n", res);

            for (auto& topic_res : res.data.responses) {
                for (auto& r : topic_res.partitions) {
                    if (r.records && !r.records->empty()) {
                        consumer->upsert_partition(
                          model::topic_partition{
                            topic_res.topic, r.partition_index},
                          r.records->last_offset() + model::offset{1});
                    }

                    while (r.records && !r.records->empty()) {
                        auto adapter = r.records->consume_batch();
                        if (!adapter.batch) {
                            continue;
                        }
                        total_count += adapter.batch->header().record_count;
                    }
                }
            }
            fmt::print("Count: {}\n", total_count);
        } catch (...) {
            fmt::print(
              "Exception while fetching: {}\n", std::current_exception());
        }
        co_await ss::sleep_abortable(app_conf.fetch_sleep_ms);
    }
}

int main(int ac, char* av[]) {
    app_configs conf{};
    int fetch_sleep_ms{0}, consumer_max_wait_ms{0};
    namespace po = boost::program_options;
    po::options_description desc("Allowed options");
    desc.add_options()("help", "Allowed options")(
      "topic", po::value<std::string>(&conf.topic), "The topic to consume")(
      "partitions",
      po::value<size_t>(&conf.n_partitions)->default_value(1),
      "The number of partitions `topic` has")(
      "fetch_sleep_ms",
      po::value<int>(&fetch_sleep_ms)->default_value(1),
      "The amount of time to sleep in the client after each fetch response")(
      "consumer_max_wait_ms",
      po::value<int>(&consumer_max_wait_ms)->default_value(1000),
      "max.wait.ms in the fetch requests");

    po::variables_map vm;
    po::store(po::parse_command_line(ac, av, desc), vm);
    po::notify(vm);

    if (vm.count("help") || vm.empty()) {
        std::cout << desc << "\n";
        return 1;
    }

    conf.fetch_sleep_ms = std::chrono::milliseconds(fetch_sleep_ms);
    conf.consumer_max_wait_ms = std::chrono::milliseconds(consumer_max_wait_ms);

    ss::app_template::seastar_options sscfg;
    sscfg.smp_opts.smp.set_value(1);
    sscfg.smp_opts.memory_allocator = ss::memory_allocator::standard;
    sscfg.reactor_opts.overprovisioned.set_value();
    sscfg.log_opts.default_log_level.set_value(ss::log_level::warn);
    ss::app_template app(std::move(sscfg));
    ss::sstring prog_name = "kc_consumer";
    std::array<char*, 1> args = {prog_name.data()};
    try {
        return app.run(args.size(), args.data(), [&]() { return run(conf); });
    } catch (...) {
        std::cerr << std::current_exception() << "\n";
        return 1;
    }
}
