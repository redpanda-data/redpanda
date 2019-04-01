#include "raft_log_service.h"

#include <flatbuffers/minireflect.h>
#include <smf/fbs_typed_buf.h>
#include <smf/log.h>

#include "filesystem/wal_core_mapping.h"
#include "filesystem/wal_segment_record.h"

namespace v {
using kt = raft::log_index_topic_key_type;

constexpr static const char *kRedpandaNamespace = "_redpanda";
constexpr static const char *kRedpandaLocalNamespace = "_redpanda_local";
constexpr static const char *kRaftConfigTopic = "raft_cfg";
constexpr static const char *kRedpandaTopicCreationTopic =
  "raft_topic_creation";

// --- static helpers

static seastar::future<std::unique_ptr<wal_read_reply>>
raft_configuration_log_read(seastar::distributed<v::write_ahead_log> *log,
                            wal_get_requestT get) {
  auto body = smf::native_table_as_buffer<wal_get_request>(get);
  auto tbuf = smf::fbs_typed_buf<wal_get_request>(std::move(body));
  auto req = wal_core_mapping::core_assignment(tbuf.get());
  auto core = req.runner_core;
  return log->invoke_on(
    req.runner_core,
    [log, tbuf = std::move(tbuf), req = std::move(req)](auto &local) mutable {
      return seastar::do_with(std::move(tbuf), std::move(req),
                              [&local](auto &tbuf, auto &req) mutable {
                                return local.get(std::move(req));
                              });
    });
}

static seastar::future<>
read_all_raft_configuration_topic(
  seastar::distributed<v::write_ahead_log> *log, int64_t ns, int64_t topic,
  int64_t start_offset, int64_t end_offset,
  std::function<seastar::future<>(std::unique_ptr<wal_read_reply>)> &&f) {
  return seastar::do_with(
    start_offset, end_offset,
    [log, ns, topic, f = std::move(f)](int64_t &begin, const int64_t &end) {
      return seastar::do_until(
        [&begin, end] { return begin >= end; },
        [&begin, log, ns, topic, f = std::move(f)]() mutable {
          wal_get_requestT get;
          get.topic = topic;
          get.ns = ns;
          get.partition = 0;
          get.offset = begin;
          get.max_bytes = 1024 * 1024 /*1MB at a time*/;
          return raft_configuration_log_read(log, std::move(get))
            .then([&begin, f = std::move(f)](auto r) {
              begin = r->reply().next_offset;
              return f(std::move(r));
            });
        });
    });
}

seastar::future<>
raft_log_service::raft_cfg_log_process_one(std::unique_ptr<wal_read_reply> r) {
  auto data = r->release();
  LOG_THROW_IF(data->error != wal_read_errno::wal_read_errno_none,
               "Error reading log entry. Unrecoverable");

  for (auto &get : data->gets) {
    auto [kbuf, vbuf] = wal_segment_record::extract_from_bin(
      reinterpret_cast<const char *>(get->data.data()), get->data.size());
    // XXX(agallego) - check for version number for key

    raft::log_index_topic_key k;
    LOG_THROW_IF(
      sizeof(k) != kbuf.size(),
      "Size missmatch for parsing key type: sizeof(k): {}, kbuf.size(): {}",
      sizeof(k), kbuf.size());
    std::memcpy(&k, kbuf.get(), sizeof(k));
    const auto idx = wal_nstpidx::gen(k.ns(), k.topic(), k.partition());

    if (auto it = omap_.find(idx); it != omap_.end()) {
      DLOG_INFO("raft cfg recovery ns/topic/partition: {}/{}/{}", k.ns(),
                k.topic(), k.partition());

      switch (k.key()) {
      case kt::log_index_topic_key_type_configuration: {
        DLOG_TRACE("{}",
                   flatbuffers::FlatBufferToString(
                     reinterpret_cast<const uint8_t *>(vbuf.get()),
                     raft::configuration_request::MiniReflectTypeTable()));
        auto &cfg = it->second.config;
        auto tbuf =
          smf::fbs_typed_buf<raft::configuration_request>(std::move(vbuf));
        auto ct = std::unique_ptr<raft::configuration_requestT>(tbuf->UnPack());
        cfg = *ct;
        break;
      }
      case kt::log_index_topic_key_type_voted_for: {
        raft::log_index_voted_for vf;
        LOG_THROW_IF(
          sizeof(vf) != vbuf.size(),
          "Size missmatch for parsing voted_for: sizeof: {} vs real: {} ",
          sizeof(vf), vbuf.size());
        std::memcpy(&vf, vbuf.get(), sizeof(vf));
        DLOG_TRACE("Recovering 'voted_for': {}", vf.voted_for());
        it->second.voted_for = vf.voted_for();
        break;
      }
      default:
        DLOG_THROW("Cannot understand key: {}, during meta log recovery",
                   k.key());
        LOG_ERROR("Could not find key in log recovery: {}", k.key());
        break;
      }
    }
  }
  return seastar::make_ready_future<>();
}

raft_log_service::raft_log_service(
  raft_cfg opts, seastar::distributed<v::write_ahead_log> *data)
  : cfg(std::move(opts)), data_(THROW_IFNULL(data)) {}

seastar::future<>
raft_log_service::initialize() {
  return initialize_cfg_lookup()
    .then([this] { return initialize_seed_servers(); })
    .then([this] { return recover_existing_logs(); });
}

seastar::future<>
raft_log_service::recover_existing_logs() {
  LOG_TRACE("Discovering persistent raft state");

  const int64_t skip_ns = xxhash_64_str(kRedpandaLocalNamespace);
  const int64_t raft_config_topic_no = xxhash_64_str(kRaftConfigTopic);

  int64_t raft_config_begin_offset = 0;
  int64_t raft_config_end_offset = 0;

  auto data_stats = data_->local().stats();
  for (auto &e : data_stats->stats) {
    auto &partition_stats = e.second;
    auto &segments = partition_stats->segments;

    if (partition_stats->ns == skip_ns) {
      if (partition_stats->topic == raft_config_topic_no) {
        raft_config_begin_offset = segments.front().start_offset();
        raft_config_end_offset = segments.back().end_offset();
      }
      // skipping the local config topic
      continue;
    }

    std::stable_sort(
      segments.begin(), segments.end(),
      [](const auto &lhs, const auto &rhs) { return lhs.term() < rhs.term(); });

    // helpful print
    std::set<int64_t> terms_print;
    for (const auto &si : segments) {
      terms_print.insert(si.term());
    }
    LOG_INFO("Raft: nstpidx:{}, found ({}) terms: {}", e.first,
             terms_print.size(),
             std::vector<int64_t>(terms_print.begin(), terms_print.end()));

    raft_nstpidx_metadata m(
      e.first, partition_stats->ns, partition_stats->topic,
      partition_stats->partition, segments.back().term(),
      segments.front().start_offset(), segments.back().end_offset());

    omap_.emplace(std::move(e.first), std::move(std::move(m)));
  }
  return read_all_raft_configuration_topic(
    data_, skip_ns, raft_config_topic_no, raft_config_begin_offset,
    raft_config_end_offset,
    [this](auto reply) { return raft_cfg_log_process_one(std::move(reply)); });
}

seastar::future<>
raft_log_service::initialize_cfg_lookup() {
  if (seastar::engine().cpu_id() != 0) {
    return seastar::make_ready_future<>();
  }
  LOG_INFO("Verifying raft configuration for ns/topic: {}/{}",
           kRedpandaLocalNamespace, kRaftConfigTopic);
  wal_topic_create_requestT x;
  x.ns = kRedpandaLocalNamespace;
  x.topic = kRaftConfigTopic;
  x.partitions = 1;
  x.type = wal_topic_type::wal_topic_type_regular;
  // XXX(agallego) - when we have per topic expiration, we should set this to
  // infinite and only do size base for this one
  auto body = smf::native_table_as_buffer<wal_topic_create_request>(x);
  auto tbuf = smf::fbs_typed_buf<wal_topic_create_request>(std::move(body));
  auto req = std::move(wal_core_mapping::core_assignment(tbuf.get())[0]);
  return seastar::do_with(
    std::move(tbuf), std::move(req), [this](auto &tbuf, auto &req) {
      auto const core = req.runner_core;
      return data_->invoke_on(
        core, [this, r = std::move(req)](auto &local) mutable {
          return local.create(std::move(r)).then([](auto _) {
            LOG_INFO("Sucessful verification for raft cfg: ns/topic: {}/{}",
                     kRedpandaLocalNamespace, kRaftConfigTopic);
            return seastar::make_ready_future<>();
          });
        });
    });
}

seastar::future<>
raft_log_service::initialize_seed_servers() {
  if (seastar::engine().cpu_id() != 0) {
    // core 0 will do the dispatch to multiple cores later
    return seastar::make_ready_future<>();
  }
  if (cfg.seeds.end() ==
      std::find_if(cfg.seeds.begin(), cfg.seeds.end(),
                   [this](auto &it) { return it.id == cfg.id; })) {
    LOG_INFO("Server: {}, not a seed servers. skipping seed server setup",
             cfg.id);
    return seastar::make_ready_future<>();
  }
  wal_topic_create_requestT x;
  x.ns = kRedpandaNamespace;
  x.topic = kRedpandaTopicCreationTopic;
  x.partitions = cfg.seed_server_meta_topic_partitions;
  x.type = wal_topic_type::wal_topic_type_regular;

  auto body = smf::native_table_as_buffer<wal_topic_create_request>(x);
  auto tbuf = smf::fbs_typed_buf<wal_topic_create_request>(std::move(body));
  auto reqs = wal_core_mapping::core_assignment(tbuf.get());
  return seastar::do_with(
    std::move(tbuf), std::move(reqs), [this](auto &tbuf, auto &reqs) {
      using iter_t = typename std::decay_t<decltype(reqs)>::iterator;
      return seastar::do_for_each(
        std::move_iterator<iter_t>(reqs.begin()),
        std::move_iterator<iter_t>(reqs.end()), [this](auto r) {
          auto const core = r.runner_core;
          return data_->invoke_on(
            core, [this, r = std::move(r)](auto &local) mutable {
              return local.create(std::move(r)).then([](auto _) {
                LOG_INFO("Sucessful verification for raft cfg: ns/topic: {}/{}",
                         kRedpandaNamespace, kRedpandaTopicCreationTopic);
                return seastar::make_ready_future<>();
              });
            });
        });
    });

  return seastar::make_ready_future<>();
}

seastar::future<>
raft_log_service::stop() {
  return cache_.close();
}
/*
seastar::future<rte<raft::append_entries_reply>>
raft_log_service::append_entries(rtc<raft::append_entries_request> && rec) {

  // process redirects
  //
}
*/

}  // namespace v
