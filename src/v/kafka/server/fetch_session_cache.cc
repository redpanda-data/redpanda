#include "kafka/server/fetch_session_cache.h"

#include "config/configuration.h"
#include "kafka/protocol/fetch.h"
#include "kafka/server/logger.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "prometheus/prometheus_sanitize.h"

#include <seastar/core/metrics.hh>

#include <chrono>

namespace kafka {

static fetch_session_partition make_fetch_partition(
  const model::topic& tp, const fetch_request::partition& p) {
    return fetch_session_partition{
      .topic = tp,
      .partition = p.partition_index,
      .max_bytes = p.max_bytes,
      .fetch_offset = p.fetch_offset,
      .high_watermark = model::offset(-1),
      .last_stable_offset = model::offset(-1),
      .current_leader_epoch = p.current_leader_epoch,
    };
}

void update_fetch_session(fetch_session& session, const fetch_request& req) {
    for (auto it = req.cbegin(); it != req.cend(); ++it) {
        auto& topic = *it->topic;
        auto& partition = *it->partition;
        model::topic_partition tp(topic.name, partition.partition_index);

        if (auto s_it = session.partitions().find(tp);
            s_it != session.partitions().end()) {
            s_it->second->partition.max_bytes = partition.max_bytes;
            s_it->second->partition.fetch_offset = partition.fetch_offset;
        } else {
            session.partitions().emplace(
              make_fetch_partition(topic.name, partition));
        }
    }

    for (auto& ft : req.data.forgotten) {
        for (auto& fp : ft.forgotten_partition_indexes) {
            model::topic_partition tp(ft.name, model::partition_id(fp));
            session.partitions().erase(
              model::topic_partition_view(ft.name, model::partition_id(fp)));
        }
    }
}

fetch_session_cache::fetch_session_cache(
  std::chrono::milliseconds eviction_timeout)
  : _min_session_id(max_sessions_per_core() * seastar::this_shard_id())
  , _max_session_id(max_sessions_per_core() + _min_session_id - 1)
  , _last_session_id(_min_session_id)
  , _session_eviction_duration(eviction_timeout) {
    register_metrics();
    _session_eviction_timer.set_callback([this] {
        gc_sessions();
        // run timer twice more offten than actual session duration to incrase
        // resolution
        _session_eviction_timer.arm(_session_eviction_duration);
    });

    _session_eviction_timer.arm(_session_eviction_duration);
}

fetch_session_ctx
fetch_session_cache::maybe_get_session(const fetch_request& req) {
    fetch_session_id session_id{req.data.session_id};
    fetch_session_epoch epoch{req.data.session_epoch};

    if (req.is_full_fetch_request()) {
        // Any session specified in a FULL fetch request will be closed.
        if (session_id != invalid_fetch_session_id) {
            if (auto it = _sessions.find(session_id); it != _sessions.end()) {
                vlog(klog.debug, "removing fetch session {}", session_id);
                _sessions_mem_usage -= it->second->mem_usage();
                _sessions.erase(it);
            }
        }
        if (epoch == final_fetch_session_epoch) {
            // If the epoch is FINAL_EPOCH, don't try to create a new
            // session.
            return fetch_session_ctx{};
        }
        // create new session
        auto new_id = new_session_id();
        if (!new_id) {
            // if we weren't able to create a session return sessionless
            // context
            return fetch_session_ctx();
        }

        auto new_session = ss::make_lw_shared<fetch_session>(*new_id);
        // initialize fetch session partitions
        update_fetch_session(*new_session, req);

        auto [it, success] = _sessions.emplace(*new_id, std::move(new_session));
        vassert(
          success,
          "fetch session {} already exists, can not insert the session",
          *new_id);

        vlog(klog.debug, "fetch session created: {}", *new_id);
        _sessions_mem_usage += it->second->mem_usage();
        return fetch_session_ctx(it->second, true);
    }
    auto it = _sessions.find(session_id);
    if (it == _sessions.end()) {
        vlog(klog.info, "no session with id {} found", session_id);
        return fetch_session_ctx(error_code::fetch_session_id_not_found);
    }

    auto session = it->second;
    if (session->epoch() != epoch) {
        vlog(
          klog.info,
          "session {} error, invalid epoch. expected: {}, got: {}",
          session_id,
          session->epoch(),
          epoch);
        return fetch_session_ctx(error_code::invalid_fetch_session_epoch);
    }
    _sessions_mem_usage -= session->mem_usage();
    update_fetch_session(*session, req);
    if (session->empty()) {
        vlog(
          klog.info,
          "session {} with epoch {} is empty, removing",
          session_id,
          session->epoch(),
          epoch);

        _sessions.erase(session_id);
        return fetch_session_ctx();
    }

    session->advance_epoch();
    _sessions_mem_usage += session->mem_usage();
    return fetch_session_ctx(session, false);
}

// we split whole range from 1 to max int32_t betewen all shards
std::optional<fetch_session_id> fetch_session_cache::new_session_id() {
    if (unlikely(
          mem_usage() > max_mem_usage
          || _sessions.size() > max_sessions_per_core())) {
        return std::nullopt;
    }

    if (_last_session_id >= _max_session_id) {
        _last_session_id = _min_session_id;
    }

    while (_sessions.contains(++_last_session_id)) {
    }

    return _last_session_id;
}

void fetch_session_cache::gc_sessions() {
    auto now = model::timeout_clock::now();
    for (auto it = _sessions.cbegin(); it != _sessions.cend();) {
        // session is in use or was used recently skip
        if (
          it->second->is_locked()
          || now - it->second->_last_used < _session_eviction_duration) {
            // do nothing
            ++it;
        } else {
            vlog(klog.debug, "evicting session {}", it->second->id());
            _sessions_mem_usage -= it->second->mem_usage();
            _sessions.erase(it++);
        }
    }
}

void fetch_session_cache::register_metrics() {
    if (config::shard_local_cfg().disable_metrics()) {
        return;
    }

    namespace sm = ss::metrics;
    _metrics.add_group(
      prometheus_sanitize::metrics_name("kafka:fetch_sessions_cache"),
      {sm::make_gauge(
         "mem_usage_bytes",
         [this] { return mem_usage(); },
         sm::description("Fetch sessions cache memory usage in bytes")),
       sm::make_gauge(
         "sessions_count",
         [this] { return _sessions.size(); },
         sm::description("Total number of fetch sessions"))});
}

} // namespace kafka
