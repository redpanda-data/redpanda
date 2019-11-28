#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

namespace config {
struct validation_error {
    validation_error(sstring name, sstring error_msg)
      : _name(std::move(name))
      , _error_msg(std::move(error_msg)) {}

    sstring name() { return _name; }

    sstring error_message() { return _error_msg; }

private:
    sstring _name;
    sstring _error_msg;
};
}; // namespace config
