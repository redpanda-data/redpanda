
#pragma once
#include <system_error>

namespace cluster {

enum class errc {
    success = 0, // must be 0
    notification_wait_timeout,
    topic_invalid_partitions,
    topic_invalid_replication_factor,
    topic_invalid_config,
    not_leader_controller,
    topic_already_exists,
    replication_error,
};
struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "cluster::errc::success";
        case errc::notification_wait_timeout:
            return "cluster::errc::notification_wait_timeout";
        case errc::topic_invalid_partitions:
            return "cluster::errc::topic_invalid_partitions";
        case errc::topic_invalid_replication_factor:
            return "cluster::errc::topic_invalid_replication_factor";
        case errc::topic_invalid_config:
            return "cluster::errc::topic_invalid_config";
        case errc::not_leader_controller:
            return "cluster::errc::not_leader_controller";
        case errc::topic_already_exists:
            return "cluster::errc::topic_already_exists";
        case errc::replication_error:
            return "cluster::errc::replication_error";
        default:
            return "cluster::errc::unknown";
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
} // namespace cluster
namespace std {
template<>
struct is_error_code_enum<cluster::errc> : true_type {};
} // namespace std