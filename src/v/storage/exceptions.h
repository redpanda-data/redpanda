#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <exception>
#include <utility>

class malformed_batch_stream_exception : public std::exception {
public:
    explicit malformed_batch_stream_exception(sstring s)
      : _msg(std::move(s)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    sstring _msg;
};
