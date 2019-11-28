#pragma once
#include <cstdint>
namespace raft {
class probe {
public:
    void vote_requested() { ++_vote_requests; }

    void vote_request_term_older() { ++_vote_request_older_term; }

    void vote_request_term_newer() { ++_vote_request_newer_term; }

    void append_requested() { ++_append_requests; }

    void entries_appended(uint32_t entries) { _appended_entries += entries; }

    void append_request_term_older() { ++_append_request_older_term; }

    void append_request_term_newer() { ++_append_request_newer_term; }

    void append_request_log_commited_index_mismatch() {
        ++_append_request_log_commited_index_mismatched;
    }

    void append_request_log_term_older() { ++_append_request_older_log_term; }

    void append_request_log_truncate() { ++_append_request_log_truncated; }

    void leader_commit_index_mismatch() { ++_leader_commit_index_mismatched; }

    void append_request_heartbeat() { ++_append_request_heartbeats; }

    void step_down() { ++_step_downs; }

private:
    uint64_t _append_requests = 0;
    uint64_t _appended_entries = 0;
    uint64_t _append_request_heartbeats = 0;
    uint32_t _append_request_newer_term = 0;
    uint32_t _append_request_older_term = 0;
    uint32_t _append_request_log_truncated = 0;
    uint32_t _append_request_older_log_term = 0;
    uint64_t _vote_requests = 0;
    uint32_t _vote_request_older_term = 0;
    uint32_t _vote_request_newer_term = 0;
    uint32_t _append_request_log_commited_index_mismatched = 0;
    uint32_t _leader_commit_index_mismatched = 0;
    uint32_t _step_downs = 0;
};
} // namespace raft
