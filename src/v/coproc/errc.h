#pragma once
#include <system_error>

namespace coproc {

enum class errc {
    success = 0,
    topic_already_enabled,
    topic_does_not_exist,
    topic_never_enabled,
    invalid_topic,
    materialized_topic,
    internal_error
};

struct errc_category final : public std::error_category {
    const char* name() const noexcept final { return "coproc::errc"; }

    std::string message(int c) const final {
        switch (static_cast<errc>(c)) {
        case errc::success:
            return "Success";
        case errc::topic_already_enabled:
            return "Topic already enabled on behalf of a coprocessor";
        case errc::topic_does_not_exist:
            return "Topic does not exist yet";
        case errc::topic_never_enabled:
            return "Topic never enabled on behalf of a coprocessor";
        case errc::invalid_topic:
            return "Topic name is invalid";
        case errc::materialized_topic:
            return "Topic is already a materialized topic";
        default:
            return "coproc::errc::internal_error check the logs";
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
} // namespace coproc
namespace std {
template<>
struct is_error_code_enum<coproc::errc> : true_type {};
} // namespace std
