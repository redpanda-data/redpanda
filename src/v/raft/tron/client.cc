// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/tron/logger.h"
#include "raft/tron/service.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "syschecks/syschecks.h"
#include "utils/hdr_hist.h"
#include "vlog.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include <string>

auto& tronlog = raft::tron::tronlog;
namespace ch = std::chrono;

void cli_opts(boost::program_options::options_description_easy_init o) {
    namespace po = boost::program_options;
    o("ip",
      po::value<std::string>()->default_value("127.0.0.1"),
      "ip to connect to");
    o("port", po::value<uint16_t>()->default_value(20776), "port for service");
    o("concurrency",
      po::value<std::size_t>()->default_value(1),
      "number of concurrent requests per TCP connection");
    o("key-size",
      po::value<std::size_t>()->default_value(100),
      "size in bytes");
    o("value-size",
      po::value<std::size_t>()->default_value(100),
      "size in bytes");
    o("parallelism",
      po::value<std::size_t>()->default_value(1),
      "number of TCP connections per core");
    o("ca-cert",
      po::value<std::string>()->default_value(""),
      "CA root certificate");
}

struct load_gen_cfg {
    std::size_t key_size;
    std::size_t value_size;
    std::size_t concurrency;
    std::size_t parallelism;
    rpc::transport_configuration client_cfg;
    ss::sharded<hdr_hist>* hist;
};

inline std::ostream& operator<<(std::ostream& o, const load_gen_cfg& cfg) {
    return o << "{'key_size':" << cfg.key_size
             << ", 'value_size':" << cfg.value_size
             << ", 'concurrency':" << cfg.concurrency
             << ", 'parallelism':" << cfg.parallelism << "}";
}

// 1. creates cfg.parallelism number of TCP connections
// 2. launches cfg.concurrency * parallelism number of requests
class client_loadgen {
public:
    using cli = rpc::client<raft::tron::trongen_client_protocol>;
    client_loadgen(load_gen_cfg cfg)
      : _cfg(std::move(cfg))
      , _mem(ss::memory::stats().total_memory() * .9) {
        vlog(tronlog.debug, "Mem for loadgen: {}", _mem.available_units());
        for (std::size_t i = 0; i < _cfg.parallelism; ++i) {
            _clients.push_back(std::make_unique<cli>(_cfg.client_cfg));
        }
    }
    ss::future<> execute_loadgen() {
        return ss::parallel_for_each(
          _clients.begin(), _clients.end(), [this](auto& c) {
              return ss::parallel_for_each(
                boost::irange(std::size_t(0), _cfg.concurrency),
                [this, &c](std::size_t) mutable { return execute_one(c); });
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
    ss::future<> execute_one(std::unique_ptr<cli>& c) {
        auto mem_sz = _cfg.key_size + _cfg.value_size + 20;
        return with_semaphore(_mem, mem_sz, [this, &c] {
            return c
              ->replicate(
                gen_entry(),
                rpc::client_opts(rpc::client_opts(rpc::no_timeout)))
              .then_wrapped([m = _cfg.hist->local().auto_measure()](auto f) {
                  try {
                      (void)f.get0();
                  } catch (...) {
                      vlog(
                        tronlog.info,
                        "Error sending payload:{}",
                        std::current_exception());
                  }
              });
        });
    }

    model::record_batch_reader gen_entry() {
        ss::circular_buffer<model::record_batch> batches;
        batches.reserve(1);
        batches.push_back(data_batch());
        return model::make_memory_record_batch_reader(std::move(batches));
    }
    model::record_batch data_batch() {
        storage::record_batch_builder bldr(
          model::record_batch_type::raft_data, _offset_index);
        bldr.add_raw_kv(rand_iobuf(_cfg.key_size), rand_iobuf(_cfg.value_size));
        ++_offset_index;
        return std::move(bldr).build();
    }
    iobuf rand_iobuf(size_t n) const {
        iobuf b;
        auto data = random_generators::gen_alphanum_string(n);
        b.append(data.data(), data.size());
        return b;
    }

    model::offset _offset_index{0};
    load_gen_cfg _cfg;
    ss::semaphore _mem;
    std::vector<std::unique_ptr<cli>> _clients;
};

inline load_gen_cfg cfg_from_opts_in_thread(
  boost::program_options::variables_map& m, ss::sharded<hdr_hist>* h) {
    rpc::transport_configuration client_cfg;
    client_cfg.server_addr = net::unresolved_address(
      m["ip"].as<std::string>(), m["port"].as<uint16_t>());
    auto ca_cert = m["ca-cert"].as<std::string>();
    if (ca_cert != "") {
        auto builder = ss::tls::credentials_builder();
        // FIXME
        // builder.set_dh_level(tls::dh_params::level::MEDIUM);
        vlog(tronlog.info, "Using {} as CA root certificate", ca_cert);
        builder.set_x509_trust_file(ca_cert, ss::tls::x509_crt_format::PEM)
          .get0();
        client_cfg.credentials
          = builder.build_reloadable_certificate_credentials().get0();
    }
    client_cfg.max_queued_bytes = ss::memory::stats().total_memory() * .8;
    return load_gen_cfg{
      .key_size = m["key-size"].as<std::size_t>(),
      .value_size = m["value-size"].as<std::size_t>(),
      .concurrency = m["concurrency"].as<std::size_t>(),
      .parallelism = m["parallelism"].as<std::size_t>(),
      .client_cfg = std::move(client_cfg),
      .hist = h};
}

inline hdr_hist aggregate_in_thread(ss::sharded<hdr_hist>& h) {
    hdr_hist retval;
    for (auto i = 0; i < ss::smp::count; ++i) {
        h.invoke_on(i, [&retval](const hdr_hist& o) { retval += o; }).get();
    }
    return retval;
}
void write_latency_in_thread(ss::sharded<hdr_hist>& hist) {
    auto h = aggregate_in_thread(hist);
    // write to file instead
    std::cout << std::endl << h << std::endl;
}

int main(int args, char** argv, char** env) {
    syschecks::initialize_intrinsics();
    std::setvbuf(stdout, nullptr, _IOLBF, 1024);
    ss::app_template app;
    cli_opts(app.add_options());
    ss::sharded<client_loadgen> client;
    ss::sharded<hdr_hist> hist;
    return app.run(args, argv, [&] {
        return ss::async([&] {
            auto& cfg = app.configuration();
            vlog(tronlog.info, "constructing histogram");
            hist.start().get();
            auto hd = ss::defer([&hist] { hist.stop().get(); });
            const load_gen_cfg lcfg = cfg_from_opts_in_thread(cfg, &hist);
            vlog(tronlog.info, "config:{}", lcfg);
            vlog(tronlog.info, "constructing client");
            client.start(lcfg).get();
            auto cd = ss::defer([&client] { client.stop().get(); });
            vlog(tronlog.info, "connecting clients");
            client.invoke_on_all(&client_loadgen::connect).get();
            vlog(tronlog.info, "invoking loadgen");
            client.invoke_on_all(&client_loadgen::execute_loadgen).get();
            vlog(tronlog.info, "writing results");
            write_latency_in_thread(hist);
            vlog(tronlog.info, "stopping");
        });
    });
}
