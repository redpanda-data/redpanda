#pragma once
#include "config/configuration.h"
#include "kafka/default_namespace.h"
#include "redpanda/application.h"
#include "storage/directories.h"
#include "storage/tests/random_batch.h"
#include "test_utils/logs.h"

#include <seastar/util/log.hh>

#include <fmt/format.h>
#include <v/native_thread_pool.h>

#include <filesystem>

class redpanda_test_fixture {
public:
    redpanda_test_fixture()
      : thread_pool(1, 1, 0) {
        thread_pool.start().get();
        app.initialize();
        configure();
        app.check_environment();
        app.configure_admin_server();
        app.wire_up_services();
        app.start();
    }

    ~redpanda_test_fixture() {
        thread_pool.stop().get();
        std::filesystem::remove_all(data_dir);
    }

    void configure() {
        data_dir = fmt::format("test_dir_{}", time(0));
        smp::invoke_on_all([this] {
            auto& config = config::shard_local_cfg();

            config.get("data_directory")
              .set_value(config::data_directory_path{.path = data_dir});

            config.get("node_id").set_value(1);

            std::vector<config::seed_server> seed_servers = {
              {1, socket_address(net::inet_address("127.0.0.1"), 33145)}};
            config.get("seed_servers").set_value(seed_servers);
        }).get0();
    }

    application app;
    v::ThreadPool thread_pool;
    std::filesystem::path data_dir;
};
