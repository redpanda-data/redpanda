// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/timeout_clock.h"
#include "rpc/demo/demo_utils.h"
#include "rpc/demo/simple_service.h"
#include "rpc/types.h"
#include "syschecks/syschecks.h"
#include "utils/hdr_hist.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <string>

namespace ch = std::chrono;
static ss::logger lgr{"demo client"};

void cli_opts(boost::program_options::options_description_easy_init o) {
    namespace po = boost::program_options;
    o("ip",
      po::value<std::string>()->default_value("127.0.0.1"),
      "ip to connect to");
    o("port", po::value<uint16_t>()->default_value(20776), "port for service");
    o("concurrency",
      po::value<std::size_t>()->default_value(1000),
      "number of concurrent requests per TCP connection");
    o("data-size",
      po::value<std::size_t>()->default_value(1 << 20),
      "1MB default data _per_ request");
    o("chunk-size",
      po::value<std::size_t>()->default_value(1 << 15),
      "fragment data_size by this chunk size step (32KB default)");
    o("parallelism",
      po::value<std::size_t>()->default_value(2),
      "number of TCP connections per core");
    o("test-case",
      po::value<std::size_t>()->default_value(1),
      "1: large payload, 2: complex struct, 3: interspersed");
    o("ca-cert",
      po::value<std::string>()->default_value(""),
      "CA root certificate");
}

struct load_gen_cfg {
    std::size_t global_total_requests() const {
        return core_total_requests() * ss::smp::count;
    }
    std::size_t global_size_test1() const {
        return global_total_requests() * data_size;
    }
    std::size_t global_size_test2() const {
        return global_total_requests() * sizeof(demo::complex_request);
    }
    std::size_t core_total_requests() const {
        return parallelism * concurrency;
    }

    std::size_t data_size;
    std::size_t chunk_size;
    std::size_t concurrency;
    std::size_t parallelism;
    std::size_t test_case;
    rpc::transport_configuration client_cfg;
    ss::sharded<hdr_hist>* hist;
};

inline std::ostream& operator<<(std::ostream& o, const load_gen_cfg& cfg) {
    // make the output json-able so we can consume it in python for analysis
    return o << "{'data_size':" << cfg.data_size
             << ", 'chunk_size':" << cfg.chunk_size
             << ", 'concurrency':" << cfg.concurrency
             << ", 'parallelism':" << cfg.parallelism
             << ", 'test_case':" << cfg.test_case
             << ", 'max_queued_bytes_per_tcp':"
             << cfg.client_cfg.max_queued_bytes
             << ", 'global_test_1_data_size':" << cfg.global_size_test1()
             << ", 'global_test_2_data_size':" << cfg.global_size_test2()
             << ", 'global_requests':" << cfg.global_total_requests()
             << ", 'cores':" << ss::smp::count << "}";
}

// 1. creates cfg.parallelism number of TCP connections
// 2. launches cfg.concurrency * parallelism number of requests
// 3. each request is cfg.data_size large
// 4. each cfg.data_size is split into cfg.chunk_size # of chunks
// 5. profit
class client_loadgen {
public:
    using cli = rpc::client<demo::simple_client_protocol>;
    client_loadgen(load_gen_cfg cfg)
      : _cfg(std::move(cfg))
      , _mem(ss::memory::stats().total_memory() * .9) {
        lgr.debug("Mem for loadgen: {}", _mem.available_units());
        for (std::size_t i = 0; i < _cfg.parallelism; ++i) {
            _clients.push_back(std::make_unique<cli>(_cfg.client_cfg));
        }
    }
    ss::future<> execute_loadgen() {
        return ss::parallel_for_each(
          _clients.begin(), _clients.end(), [this](auto& c) {
              return ss::parallel_for_each(
                boost::irange(std::size_t(0), _cfg.concurrency),
                [this, &c](std::size_t) mutable {
                    return execute_one(c.get());
                });
          });
    }
    ss::future<> connect() {
        return ss::parallel_for_each(
          _clients.begin(), _clients.end(), [](auto& c) {
              return c->connect(model::no_timeout);
          });
    }
    ss::future<> stop() {
        return ss::parallel_for_each(
          _clients.begin(), _clients.end(), [](auto& c) { return c->stop(); });
    }

private:
    ss::future<> execute_one(cli* const c) {
        if (_cfg.test_case < 1 || _cfg.test_case > 3) {
            throw std::runtime_error(fmt::format(
              "Unknown test:{}, bad config:{}", _cfg.test_case, _cfg));
        }
        if (_cfg.test_case == 1) {
            return get_units(_mem, _cfg.data_size)
              .then([this, c](ss::semaphore_units<> u) {
                  return c
                    ->put(
                      demo::gen_simple_request(_cfg.data_size, _cfg.chunk_size),
                      rpc::client_opts(rpc::no_timeout))
                    .then([m = _cfg.hist->local().auto_measure(),
                           u = std::move(u)](auto _) {});
              });
        } else if (_cfg.test_case == 2) {
            return get_units(_mem, sizeof(demo::complex_request{}))
              .then([this, &c](ss::semaphore_units<> u) {
                  return c
                    ->put_complex(
                      demo::complex_request{},
                      rpc::client_opts(rpc::no_timeout))
                    .then([m = _cfg.hist->local().auto_measure(),
                           u = std::move(u)](auto _) {});
              });
        } else if (_cfg.test_case == 3) {
            return get_units(_mem, _cfg.data_size)
              .then([this, &c](ss::semaphore_units<> u) {
                  auto r = demo::gen_interspersed_request(
                    _cfg.data_size, _cfg.chunk_size);
                  return c
                    ->put_interspersed(
                      std::move(r), rpc::client_opts(rpc::no_timeout))
                    .then([m = _cfg.hist->local().auto_measure(),
                           u = std::move(u)](auto _) {});
              });
        }
        __builtin_unreachable();
    }

    load_gen_cfg _cfg;
    ss::semaphore _mem;
    std::vector<std::unique_ptr<cli>> _clients;
};

inline load_gen_cfg
cfg_from(boost::program_options::variables_map& m, ss::sharded<hdr_hist>* h) {
    rpc::transport_configuration client_cfg;
    client_cfg.server_addr = net::unresolved_address(
      m["ip"].as<std::string>(), m["port"].as<uint16_t>());
    auto ca_cert = m["ca-cert"].as<std::string>();
    if (ca_cert != "") {
        auto builder = ss::tls::credentials_builder();
        // FIXME
        // builder.set_dh_level(tls::dh_params::level::MEDIUM);
        vlog(lgr.info, "Using {} as CA root certificate", ca_cert);
        builder.set_x509_trust_file(ca_cert, ss::tls::x509_crt_format::PEM)
          .get0();
        client_cfg.credentials
          = builder.build_reloadable_certificate_credentials().get0();
    }
    client_cfg.max_queued_bytes = ss::memory::stats().total_memory() * .8;
    return load_gen_cfg{
      .data_size = m["data-size"].as<std::size_t>(),
      .chunk_size = m["chunk-size"].as<std::size_t>(),
      .concurrency = m["concurrency"].as<std::size_t>(),
      .parallelism = m["parallelism"].as<std::size_t>(),
      .test_case = m["test-case"].as<std::size_t>(),
      .client_cfg = std::move(client_cfg),
      .hist = h};
}

class throughput {
public:
    using time_t = std::chrono::steady_clock::time_point;
    throughput(std::size_t total_requests)
      : _total_requests(total_requests)
      , _begin(now()) {}
    double qps() const {
        if (_end < _begin) {
            throw std::runtime_error("call ::stop() first");
        }
        double d = static_cast<double>(duration_ms());
        d /= 1000.0;
        return static_cast<double>(_total_requests) / d;
    }
    void stop() {
        if (_end > _begin) {
            throw std::runtime_error("throughput time already stopped");
        }
        _end = now();
    }
    std::size_t duration_ms() const {
        return ch::duration_cast<ch::milliseconds>(_end - _begin).count();
    }

private:
    time_t now() const { return std::chrono::steady_clock::now(); }
    std::size_t _total_requests;
    time_t _begin;
    time_t _end;
};

inline std::ostream& operator<<(std::ostream& o, const throughput& t) {
    return o << "{'qps':" << t.qps() << ",'duration_ms':" << t.duration_ms()
             << "}";
}

inline hdr_hist aggregate_in_thread(ss::sharded<hdr_hist>& h) {
    hdr_hist retval;
    for (auto i = 0; i < ss::smp::count; ++i) {
        h.invoke_on(i, [&retval](const hdr_hist& o) { retval += o; }).get();
    }
    return retval;
}

void write_configuration_in_thread(
  const throughput& tp, const load_gen_cfg& cfg) {
    std::ostringstream to;
    to << "{'throughput':" << tp << ", 'config':" << cfg << "}";
    const ss::sstring s = to.str();
    force_write_ptr("test_config.json", s.data(), s.size()).get();
}

void write_latency_in_thread(ss::sharded<hdr_hist>& hist) {
    auto h = aggregate_in_thread(hist);
    write_histogram("clients.hdr", h).get();
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app;
    cli_opts(app.add_options());
    ss::sharded<client_loadgen> client;
    ss::sharded<hdr_hist> hist;
    return app.run(args, argv, [&] {
        auto& cfg = app.configuration();
        return ss::async([&] {
            vlog(lgr.info, "constructing histogram");
            hist.start().get();
            auto hd = ss::defer([&hist] { hist.stop().get(); });
            const load_gen_cfg lcfg = cfg_from(cfg, &hist);
            vlog(lgr.info, "config:{}", lcfg);
            vlog(lgr.info, "constructing client");
            client.start(lcfg).get();
            auto cd = ss::defer([&client] { client.stop().get(); });
            vlog(lgr.info, "connecting clients");
            client.invoke_on_all(&client_loadgen::connect).get();
            auto tp = throughput(lcfg.global_total_requests());
            vlog(lgr.info, "invoking loadgen");
            client.invoke_on_all(&client_loadgen::execute_loadgen).get();
            tp.stop();
            vlog(lgr.info, "{}", tp);
            vlog(lgr.info, "writing results");
            write_configuration_in_thread(tp, lcfg);
            write_latency_in_thread(hist);
            vlog(lgr.info, "stopping");
        });
    });
}
