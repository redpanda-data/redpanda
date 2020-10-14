#pragma once
#include "cluster/namespace.h"
#include "cluster/types.h"
#include "kafka/client.h"
#include "kafka/requests/topics/topic_utils.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "redpanda/application.h"
#include "storage/directories.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "test_utils/logs.h"

#include <seastar/util/log.hh>

#include <fmt/format.h>

#include <filesystem>

struct redpanda_thread_fixture_opts {
    bool enable_pid_file{false};
    bool developer_mode{true};
    bool enable_admin_api{false};
    bool enable_coproc{false};
    bool disable_metrics{true};
    int node_id{1};
};

class redpanda_thread_fixture {
public:
    static constexpr const char* rack_name = "i-am-rack";

    redpanda_thread_fixture(
      redpanda_thread_fixture_opts opts = redpanda_thread_fixture_opts()) {
        configure(opts);
        app.initialize();
        app.check_environment();
        app.configure_admin_server();
        app.wire_up_services();
        app.start();
    }

    ~redpanda_thread_fixture() {
        app.shutdown();
        std::filesystem::remove_all(data_dir);
    }

    config::configuration& lconf() { return config::shard_local_cfg(); }

    void configure(redpanda_thread_fixture_opts opts) {
        data_dir = fmt::format("test.dir_{}", time(0));
        ss::smp::invoke_on_all([this, opts] {
            auto& config = config::shard_local_cfg();
            config.get("enable_pid_file").set_value(opts.enable_pid_file);
            config.get("developer_mode").set_value(opts.developer_mode);
            config.get("enable_admin_api").set_value(opts.enable_admin_api);
            config.get("enable_coproc").set_value(opts.enable_coproc);
            config.get("rack").set_value(std::optional<ss::sstring>(rack_name));
            config.get("disable_metrics").set_value(opts.disable_metrics);

            config.get("data_directory")
              .set_value(config::data_directory_path{.path = data_dir});

            config.get("node_id").set_value(opts.node_id);
        }).get0();
    }

    ss::future<> wait_for_controller_leadership() {
        return app.controller->get_partition_leaders()
          .local()
          .wait_for_leader(
            cluster::controller_ntp,
            ss::lowres_clock::now() + std::chrono::seconds(10),
            {})
          .discard_result();
    }

    ss::future<kafka::client> make_kafka_client() {
        return config::shard_local_cfg().kafka_api().resolve().then(
          [](ss::socket_address addr) {
              return kafka::client(rpc::base_transport::configuration{
                .server_addr = addr,
              });
          });
    }

    model::ntp
    make_default_ntp(model::topic topic, model::partition_id partition) {
        return model::ntp(
          cluster::kafka_namespace, std::move(topic), partition);
    }

    storage::log_config make_default_config() {
        return storage::log_config(
          storage::log_config::storage_type::disk,
          lconf().data_directory().as_sstring(),
          1_GiB,
          storage::debug_sanitize_files::yes);
    }

    ss::future<> add_topic(model::topic_namespace_view tp_ns) {
        std::vector<cluster::topic_configuration> cfgs{
          cluster::topic_configuration(tp_ns.ns, tp_ns.tp, 1, 1)};
        return app.controller->get_topics_frontend()
          .local()
          .create_topics(std::move(cfgs), model::no_timeout)
          .then([this](std::vector<cluster::topic_result> results) {
              return tests::cooperative_spin_wait_with_timeout(
                2s, [this, results = std::move(results)] {
                    return std::all_of(
                      results.begin(),
                      results.end(),
                      [this](const cluster::topic_result& r) {
                          auto md = app.metadata_cache.local()
                                      .get_topic_metadata(r.tp_ns);
                          return md
                                 && std::all_of(
                                   md->partitions.begin(),
                                   md->partitions.end(),
                                   [this,
                                    &r](const model::partition_metadata& p) {
                                       return app.shard_table.local().shard_for(
                                         model::ntp(
                                           r.tp_ns.ns, r.tp_ns.tp, p.id));
                                   });
                      });
                });
          });
    }

    model::ntp make_data(storage::ntp_config::ntp_id version) {
        auto topic_name = fmt::format("my_topic_{}", 0);
        model::ntp ntp(
          cluster::kafka_namespace,
          model::topic(topic_name),
          model::partition_id(0));

        storage::ntp_config ntp_cfg(
          ntp, lconf().data_directory().as_sstring(), nullptr, version);

        storage::disk_log_builder builder(make_default_config());
        using namespace storage; // NOLINT
        builder | start(std::move(ntp_cfg)) | add_segment(model::offset(0))
          | add_random_batches(
            model::offset(0), 20, maybe_compress_batches::yes)
          | stop();

        add_topic(model::topic_namespace_view(ntp)).get();

        return ntp;
    }

    application app;
    std::filesystem::path data_dir;
};
