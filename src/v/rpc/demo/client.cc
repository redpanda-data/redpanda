#include "rpc/demo/types.h"
#include "seastarx.h"
#include "utils/hdr_hist.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/thread.hh>

#include <string>

namespace ch = std::chrono;
static logger lgr{"demo client"};

void cli_opts(boost::program_options::options_description_easy_init o) {
    namespace po = boost::program_options;
    o("ip",
      po::value<std::string>()->default_value("127.0.0.1"),
      "ip to connect to");
    o("port", po::value<uint16_t>()->default_value(20776), "port for service");
    o("concurrency",
      po::value<uint32_t>()->default_value(1000),
      "number of concurrent requests per TCP connection");
    o("parallelism",
      po::value<uint32_t>()->default_value(1000),
      "number of TCP connections per core");
    o("ca-cert",
      po::value<std::string>()->default_value(""),
      "CA root certificate");
}

class client_loadgen {
public:
    using cli = demo::simple_service::client;
    client_loadgen(
      uint32_t concurrency,
      uint32_t parallelism,
      rpc::client_configuration cfg,
      sharded<hdr_hist>& hist)
      : _cfg(cfg)
      , _concurrency(concurrency)
      , _parallelism(parallelism)
      , _hist(hist) {
        for (uint32_t i = 0; i < _parallelism; ++i) {
            _clients.push_back(std::make_unique<cli>(_cfg));
        }
    }
    future<> execute_loadgen() {
        return parallel_for_each(
          _clients.begin(), _clients.end(), [this](auto& c) {
              return parallel_for_each(
                boost::irange(uint32_t(0), _concurrency),
                [this, &c](uint32_t) mutable {
                    return c->fourty_two({}).discard_result().finally(
                      [m = _hist.local().auto_measure()] {});
                });
          });
    }
    future<> connect() {
        return parallel_for_each(_clients.begin(), _clients.end(), [](auto& c) {
            return c->connect();
        });
    }
    future<> stop() {
        return parallel_for_each(
          _clients.begin(), _clients.end(), [](auto& c) { return c->stop(); });
    }

private:
    const rpc::client_configuration _cfg;
    const uint32_t _concurrency;
    const uint32_t _parallelism;
    sharded<hdr_hist>& _hist;
    std::vector<std::unique_ptr<cli>> _clients;
};
int main(int args, char** argv, char** env) {
    app_template app;
    cli_opts(app.add_options());
    sharded<client_loadgen> client;
    sharded<hdr_hist> hist;
    return app.run(args, argv, [&] {
        auto& cfg = app.configuration();
    return async([&] {
        rpc::client_configuration client_cfg;
        client_cfg.server_addr = socket_address(ipv4_addr(
            cfg["ip"].as<std::string>(), cfg["port"].as<uint16_t>()));
        auto ca_cert = cfg["ca-cert"].as<std::string>();
        if (ca_cert != "" ) {
            auto builder = tls::credentials_builder();
            //builder.set_dh_level(tls::dh_params::level::MEDIUM);
            lgr.info("Using {} as CA root certificate", ca_cert);
            builder.set_x509_trust_file(ca_cert, tls::x509_crt_format::PEM)
                .get0();
            client_cfg.credentials = std::move(builder);
        }
        const uint32_t parallelism = cfg["parallelism"].as<uint32_t>();
        const uint32_t concurrency = cfg["concurrency"].as<uint32_t>();
        const uint64_t total_requests = parallelism * concurrency * smp::count;
        client
          .start(concurrency, parallelism, client_cfg, std::ref(hist))
          .then([&hist] { return hist.start(); })
          .then([&client] {
              lgr.info("Connecting to server");
              return client.invoke_on_all(&client_loadgen::connect);
          })
          .then([&client, total_requests, parallelism, concurrency] {
              lgr.info(
                "executing {} requests. {} per tcp connection ({}) per "
                "core({})",
                total_requests,
                concurrency,
                parallelism,
                smp::count);
              auto b = rpc::clock_type::now();
              return client.invoke_on_all(&client_loadgen::execute_loadgen)
                .finally([b, total_requests] {
                    auto e = rpc::clock_type::now();
                    double d
                      = ch::duration_cast<ch::milliseconds>(e - b).count();
                    d /= 1000.0;
                    lgr.info(
                      "{} requests finished in {}secs. qps:{}",
                      total_requests,
                      d,
                      int64_t(total_requests / d));
                });
          })
          .then([&hist] {
              return hist.invoke_on_all(
                [](auto& h) { lgr.info("latency: {}", h); });
          })
          .then([&hist] { return hist.stop(); })
          .then([&client] {
              lgr.info("stopping");
              return client.stop();
           }).get();
    });
    });
}
