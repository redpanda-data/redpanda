#include "cluster/partition.h"

#include "cluster/logger.h"
#include "prometheus/prometheus_sanitize.h"

namespace cluster {

partition::partition(consensus_ptr r)
  : _raft(r) {
    if (_raft->log_config().is_collectable()) {
        _nop_stm = std::make_unique<raft::log_eviction_stm>(
          _raft.get(), clusterlog, _as);
    }
}

ss::future<> partition::start() {
    if (!config::shard_local_cfg().disable_metrics()) {
        namespace sm = ss::metrics;

        auto ns_label = sm::label("namespace");
        auto topic_label = sm::label("topic");
        auto partition_label = sm::label("partition");
        auto ntp = _raft->ntp();

        const std::vector<sm::label_instance> labels = {
          ns_label(ntp.ns()),
          topic_label(ntp.tp.topic()),
          partition_label(ntp.tp.partition()),
        };

        _metrics.add_group(
          prometheus_sanitize::metrics_name("cluster:partition"),
          {
            sm::make_gauge(
              "leader",
              [this] { return is_leader() ? 1 : 0; },
              sm::description(
                "Flag indicating if this partition instance is a leader"),
              labels),
            sm::make_gauge(
              "last_stable_offset",
              [this] { return last_stable_offset(); },
              sm::description("Last stable offset"),
              labels),
            sm::make_gauge(
              "committed_offset",
              [this] { return committed_offset(); },
              sm::description("Committed offset"),
              labels),
          });
    }

    auto f = _raft->start();

    if (_nop_stm != nullptr) {
        return f.then([this] { return _nop_stm->start(); });
    }

    return f;
}

ss::future<> partition::stop() {
    _as.request_abort();
    // no state machine
    if (_nop_stm == nullptr) {
        return ss::now();
    }

    return _nop_stm->stop();
}

ss::future<std::optional<storage::timequery_result>>
partition::timequery(model::timestamp t, ss::io_priority_class p) {
    storage::timequery_config cfg(t, _raft->committed_offset(), p);
    return _raft->timequery(cfg);
}

std::ostream& operator<<(std::ostream& o, const partition& x) {
    return o << x._raft;
}
} // namespace cluster
