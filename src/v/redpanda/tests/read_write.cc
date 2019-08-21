#include "filesystem/wal_segment_record.h"
#include "redpanda/api/client.h"
#include "redpanda/redpanda_service.h"
#include "redpanda/rpc_config.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>

#include <smf/log.h>
#include <smf/lz4_filter.h>
#include <smf/random.h>
#include <smf/rpc_server.h>
#include <smf/zstd_filter.h>

#include <flatbuffers/minireflect.h>

#include <chrono>
#include <iostream>

constexpr static const int kMethodIterations = 25;
constexpr static const int kTopicPartitions = 3;

future<> writes(api::client* api) {
    return do_for_each(
      boost::counting_iterator<int>(0),
      boost::counting_iterator<int>(kMethodIterations),
      [api](auto i) {
          auto txn = api->create_txn();
          auto k = to_sstring(i) + "-hello";
          auto v = to_sstring(i) + "-world";
          LOG_INFO("Put: {}={}", k, v);
          txn.stage(k.data(), k.size(), v.data(), v.size());
          return txn.submit().then([i](auto r) {
              DLOG_TRACE_IF(
                r,
                "{}",
                flatbuffers::FlatBufferToString(
                  (const uint8_t*)r.ctx.value().payload.get(),
                  chains::chain_put_reply::MiniReflectTypeTable()));
              /*ignore?*/
          });
      });
}
future<> reads(api::client* api) {
    return do_for_each(
      boost::counting_iterator<int>(0),
      boost::counting_iterator<int>(kMethodIterations),
      [api](auto i) {
          return api->consume().then([i](auto r) {
              if (r) {
                  for (auto x : *r->get()->gets()) {
                      if (x->data() && x->data()->size() != 0) {
                          auto [kbuf, vbuf]
                            = wal_segment_record::extract_from_bin(
                              (const char*)x->data()->data(),
                              x->data()->size());
                          sstring k(kbuf.get(), kbuf.size());
                          sstring v(vbuf.get(), vbuf.size());
                          LOG_INFO(
                            "(consume call: {}) {} {} {}, next_offset: {}; "
                            "{}={}",
                            i,
                            r->get()->ns(),
                            r->get()->topic(),
                            r->get()->partition(),
                            r->get()->next_offset(),
                            k,
                            v);
                      }
                  }
              } else {
                  LOG_ERROR("(consume call: {}) no data");
              }
          });
      });
}

future<> launch_client_read_write() {
    smf::random rand;
    api::client_opts co(
      "happy-home-namespace", "happy-home-topic", rand.next(), rand.next());
    // do multiple calls/don't read too fast !
    co.consumer_max_read_bytes = 38;
    co.topic_partitions = kTopicPartitions;
    auto api = std::make_unique<api::client>(std::move(co));
    auto ptr = api.get();
    return ptr->open({"127.0.0.1", 33145})
      .then([ptr] { return writes(ptr); })
      .then([] {
          // trickster!
          // turns out that the api flushes every 30 secs
          // so for this test we change to 2ms
          // and wait 10ms for safety of test
          return sleep(std::chrono::milliseconds(30));
      })
      .then([ptr] { return reads(ptr); })
      .then([api = std::move(api), ptr]() mutable {
          return ptr->close().finally([a = std::move(api)] {});
      });
}

int main(int argc, char** argv, char** env) {
    // flush every log line
    std::cout.setf(std::ios::unitbuf);
    distributed<smf::rpc_server> rpc;
    distributed<write_ahead_log> log;
    app_template app;
    auto rp_config = config::configuration();
    rp_config.data_directory(".");
    rp_config.rpc_server(
      socket_address(seastar::net::inet_address("127.0.0.1"), 33145));
    rp_config.writer_flush_period_ms(2);
    return app.run(argc, argv, [&] {
        // smf::app_run_log_level(log_level::trace);
        engine().at_exit([&log] { return log.stop(); });
        engine().at_exit([&rpc] { return rpc.stop(); });
        auto& config = app.configuration();
        return log.start(wal_opts(rp_config))
          .then([&log] { return log.invoke_on_all(&write_ahead_log::open); })
          .then([&log] { return log.invoke_on_all(&write_ahead_log::index); })
          .then([&rpc, &rp_config] { return rpc.start(rpc_config(rp_config)); })
          .then([&rpc, config, &log] {
              return rpc.invoke_on_all([&](smf::rpc_server& s) {
                  using srvc = redpanda_service;
                  using lz4_c_t = smf::lz4_compression_filter;
                  using lz4_d_t = smf::lz4_decompression_filter;
                  using zstd_d_t = smf::zstd_decompression_filter;
                  s.register_outgoing_filter<lz4_c_t>(1024);
                  s.register_incoming_filter<lz4_d_t>();
                  s.register_incoming_filter<zstd_d_t>();
                  s.register_service<srvc>(&log);
              });
          })
          .then([&rpc] { return rpc.invoke_on_all(&smf::rpc_server::start); })
          .then([] { return launch_client_read_write(); });
    });
    return 0;
}
