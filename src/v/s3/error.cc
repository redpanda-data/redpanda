#include "s3/error.h"

namespace s3 {

struct s3_error_category final : std::error_category {
    const char* name() const noexcept final { return "s3"; }
    std::string message(int ec) const final {
        switch (static_cast<s3_error_codes>(ec)) {
        case s3_error_codes::invalid_uri:
            return "Target URI shouldn't be empty or include domain name";
        case s3_error_codes::invalid_uri_params:
            return "Target URI contains invalid query parameters";
        case s3_error_codes::not_enough_arguments:
            return "Can't make request, not enough arguments";
        }
        return "unknown";
    }
};

std::error_code make_error_code(s3_error_codes ec) noexcept {
    static s3_error_category ecat;
    return {static_cast<int>(ec), ecat};
}

} // namespace s3
