#include "redpanda/bamboo/cli.h"

#include <seastar/core/reactor.hh>

#include <smf/human_bytes.h>
#include <smf/log.h>

#include <boost/program_options.hpp>
#include <flatbuffers/minireflect.h>

#include <algorithm>

static const sstring kRegularTopic = "regular";
static const sstring kCompactionTopic = "compaction";

cli::cli(const boost::program_options::variables_map* cfg)
  : opts(THROW_IFNULL(cfg)) {
    _id = _rand.next();
    api::client_opts co(
      options()["namespace"].as<sstring>(),
      options()["topic"].as<sstring>(),
      _rand.next(),
      _rand.next());
    co.server_side_verify_payload
      = options()["server-side-verify-checksum"].as<bool>();
    if (auto tpc = options()["partitions-per-topic"].as<int32_t>(); tpc > 0) {
        co.topic_partitions = tpc;
    }
    co.enable_detailed_latency_metrics
      = options()["enable-histogram"].as<bool>();
    if (kRegularTopic == options()["topic-type"].as<sstring>()) {
        co.topic_type = wal_topic_type::wal_topic_type_regular;
    }
    if (kCompactionTopic == options()["topic-type"].as<sstring>()) {
        co.topic_type = wal_topic_type::wal_topic_type_compaction;
    }
    _api = std::make_unique<api::client>(std::move(co));
    write_key_sz_ = options()["key-size"].as<int32_t>();
    write_val_sz_ = options()["value-size"].as<int32_t>();
    write_batch_sz_ = options()["write-batch-size"].as<int32_t>();
    partition_pref_ = options()["partition"].as<int32_t>();
}
cli::~cli() {
    if (_api) {
        auto& x = _api->stats();
        DLOG_TRACE(
          "cli::id({}): bytes_sent: {}, bytes_read: {}, "
          "read_rpc:{}, write_rpc:{}",
          _id,
          smf::human_bytes(x.bytes_sent),
          smf::human_bytes(x.bytes_read),
          x.read_rpc,
          x.write_rpc);
    }
}
future<> cli::one_write() {
    auto txn = _api->create_txn();
    auto k = _rand.next_alphanum(write_key_sz_);
    auto v = _rand.next_alphanum(write_val_sz_);

    auto min_rot = std::min<std::size_t>(k.size() - 1, 5);
    for (auto n = 0; n < write_batch_sz_; ++n) {
        std::rotate(k.begin(), k.begin() + min_rot, k.end());
        txn.stage(k.data(), k.size(), v.data(), v.size());
    }
    return txn.submit().then([](auto r) {
        DLOG_TRACE_IF(
          r,
          "{}",
          flatbuffers::FlatBufferToString(
            (const uint8_t*)r.ctx.value().payload.get(),
            chains::chain_put_reply::MiniReflectTypeTable()));
        /*ignore?*/
    });
}
future<> cli::one_read() {
    return _api->consume(partition_pref_).then([](auto r) {
        DLOG_TRACE_IF(
          r,
          "{} {} {}, next_offset: {}",
          r->get()->ns(),
          r->get()->topic(),
          r->get()->partition(),
          r->get()->next_offset());
        /*ignore?*/
    });
}

future<> cli::open() {
    auto ip = options()["ip"].as<sstring>();
    auto port = options()["port"].as<uint16_t>();
    auto addr = ipv4_addr(ip, port);
    return _api->open(addr).finally(
      [this] { _api->enable_histogram_metrics(); });
}
future<> cli::stop() {
    if (_api) {
        return _api->close();
    }
    return make_ready_future<>();
}

const boost::program_options::variables_map& cli::options() const {
    return *opts;
}

api::client* cli::api() const {
    return _api.get();
}
