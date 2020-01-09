#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

namespace config {
struct validation_error {
    validation_error(ss::sstring name, ss::sstring error_msg)
      : _name(std::move(name))
      , _error_msg(std::move(error_msg)) {}

    ss::sstring name() { return _name; }

    ss::sstring error_message() { return _error_msg; }

private:
    ss::sstring _name;
    ss::sstring _error_msg;
};
}; // namespace config
