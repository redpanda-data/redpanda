#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <exception>

namespace rpc {
class request_timeout_exception final : std::exception {
public:
    explicit request_timeout_exception(ss::sstring what)
      : what_(std::move(what)) {}

    const char* what() const noexcept final { return what_.c_str(); }

private:
    ss::sstring what_;
};
} // namespace rpc
