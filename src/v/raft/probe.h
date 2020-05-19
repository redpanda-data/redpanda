#pragma once
#include "raft/types.h"

#include <seastar/core/metrics_registration.hh>

#include <cstdint>
namespace raft {
class probe {
public:
    void vote_request() { ++_vote_requests; }
    void append_request() { ++_append_requests; }

    void vote_request_sent() { ++_vote_requests_sent; }

    void replicate_requests_ack_all() { ++_replicate_requests_ack_all; }
    void replicate_requests_ack_leader() { ++_replicate_requests_ack_leader; }
    void replicate_requests_ack_none() { ++_replicate_requests_ack_none; }
    void replicate_done() { ++_replicate_requests_done; }

    void log_truncated() { ++_log_truncations; }
    void log_flushed() { ++_log_flushes; }

    void replicate_batch_flushed() { ++_replicate_batch_flushed; }
    void recovery_append_request() { ++_recovery_requests; }
    void configuration_update() { ++_configuration_updates; }

    void leadership_changed() { ++_leadership_changes; }

    static std::vector<ss::metrics::label_instance>
    create_metric_labels(const model::ntp& ntp);

    void setup_metrics(const model::ntp& ntp);

private:
    uint64_t _vote_requests = 0;
    uint64_t _append_requests = 0;
    uint64_t _vote_requests_sent = 0;
    uint64_t _replicate_requests_ack_all = 0;
    uint64_t _replicate_requests_ack_leader = 0;
    uint64_t _replicate_requests_ack_none = 0;
    uint64_t _replicate_requests_done = 0;
    uint64_t _log_flushes = 0;
    uint64_t _replicate_batch_flushed = 0;
    uint32_t _log_truncations = 0;
    uint32_t _configuration_updates = 0;
    uint64_t _recovery_requests = 0;
    uint64_t _leadership_changes = 0;

    ss::metrics::metric_groups _metrics;
};
} // namespace raft
