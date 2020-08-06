#include "model/validation.h"

#include "model/errc.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/util/log.hh>

namespace model {

std::error_code validate_kafka_topic_name(const model::topic& tpc) {
    static constexpr size_t kafka_max_topic_name_length = 249;
    const auto& name = tpc();
    if (name.empty()) {
        return make_error_code(errc::topic_name_empty);
    }
    if (name == "." || name == "..") {
        return make_error_code(errc::forbidden_topic_name);
    }
    if (name.length() > kafka_max_topic_name_length) {
        return make_error_code(errc::topic_name_len_exceeded);
    }
    for (char c : name) {
        if (!std::isalnum(c) && !(c == '.' || c == '-' || c == '_')) {
            return make_error_code(errc::invalid_topic_name);
        }
    }
    return make_error_code(errc::success);
}

} // namespace model
