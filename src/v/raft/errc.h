#pragma once

#include <system_error>

namespace raft {

enum class errc {
    success = 0, // must be 0
    disconnected_endpoint,
    exponential_backoff,
    non_majority_replication,
    not_leader,
    vote_dispatch_error,
    append_entries_dispatch_error,
    replicated_entry_truncated,
    leader_flush_failed,
    leader_append_failed,
    timeout,
    configuration_change_in_progress
};
struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "raft::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "raft::errc::success";
        case errc::disconnected_endpoint:
            return "raft::errc::disconnected_endpoint(node down)";
        case errc::exponential_backoff:
            return "raft::errc::exponential_backoff";
        case errc::non_majority_replication:
            return "raft::errc::non_majority_replication";
        case errc::not_leader:
            return "raft::errc::not_leader";
        case errc::vote_dispatch_error:
            return "raft::errc::vote_dispatch_error";
        case errc::append_entries_dispatch_error:
            return "raft::errc::append_entries_dispatch_error";
        case errc::replicated_entry_truncated:
            return "raft::errc::replicated_entry_truncated";
        case errc::leader_flush_failed:
            return "raft::errc::leader_flush_failed";
        case errc::leader_append_failed:
            return "raft::errc::leader_append_failed";
        case errc::timeout:
            return "raft::errc::timeout";
        case errc::configuration_change_in_progress:
            return "raft::errc::configuration_change_in_progress";
        default:
            return "raft::errc::unknown";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static errc_category e;
    return e;
}
inline std::error_code make_error_code(errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace raft
namespace std {
template<>
struct is_error_code_enum<raft::errc> : true_type {};
} // namespace std
