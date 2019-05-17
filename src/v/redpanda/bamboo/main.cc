#include <iostream>
#include <limits>

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <smf/histogram_seastar_utils.h>
#include <smf/log.h>
#include <smf/unique_histogram_adder.h>

// bamboo
#include "cli.h"
#include "qps_load.h"

void
cli_opts(boost::program_options::options_description_easy_init o) {
  namespace po = boost::program_options;
  o("ip", po::value<seastar::sstring>()->default_value("127.0.0.1"),
    "ip to connect to");

  o("port", po::value<uint16_t>()->default_value(33145), "port for service");

  o("write-batch-size", po::value<int32_t>()->default_value(100),
    "number of items in one chain::put() rpc call");

  o("qps", po::value<int32_t>()->default_value(300),
    "number of request per second measured with coordinated ommision");

  o("seconds-duration", po::value<int32_t>()->default_value(10),
    "number of seconds to send qps - see qps");

  o("concurrency", po::value<int32_t>()->default_value(3),
    "number of green threads per real thread (seastar::futures<>)");

  o("topic", po::value<seastar::sstring>()->default_value("rickenbacker"),
    "topic name; rickenbacker is a cool bridge in Miami FL.");

  o("namespace", po::value<seastar::sstring>()->default_value("nemo"),
    "namespace name for topic");

  o("key-size", po::value<int32_t>()->default_value(50),
    "byte size of key; autogen rand bytes");

  o("value-size", po::value<int32_t>()->default_value(50),
    "byte size of value; autogen rand bytes");

  o("enable-histogram", po::value<bool>()->default_value(true),
    "enable writing the latency histogram of API requests");

  o("server-side-verify-checksum", po::value<bool>()->default_value(true),
    "checksum the payload before returning to client the data");

  o("topic-type", po::value<seastar::sstring>()->default_value("regular"),
    "'regular' or 'compaction'");

  o("rw-balance", po::value<double>()->default_value(0.5),
    "Valid between 0.0 -> 1.0; if 1.0 only do write worload");

  o("partitions-per-topic", po::value<int32_t>()->default_value(-1),
    "override partitions per topic, for topic creation");

  o("partition", po::value<int32_t>()->default_value(-1), "override partition");
}

int
main(int argc, char **argv, char **env) {
  std::setvbuf(stdout, nullptr, _IOLBF, 1024);
  seastar::distributed<qps_load> runner;
  seastar::app_template app;

  cli_opts(app.add_options());
  return app.run(argc, argv, [&runner, &app] {
    seastar::engine().at_exit([&runner] { return runner.stop(); });
    const auto &cfg = app.configuration();
    return runner.start(&cfg)
      .then([&runner] { return runner.invoke_on_all(&qps_load::open); })
      .then([&runner] { return runner.invoke_on_all(&qps_load::drive); })
      .then([&runner] {
        return runner
          .map_reduce(smf::unique_histogram_adder(),
                      [](auto &shard) { return shard.copy_histogram(); })
          .then([](auto h) {
            LOG_INFO("Histogram: bamboo.hgrm");
            return smf::histogram_seastar_utils::write("bamboo.hgrm",
                                                       std::move(h));
          });
      })
      .then([] { return seastar::make_ready_future<int>(0); });
  });
  return 0;
}
