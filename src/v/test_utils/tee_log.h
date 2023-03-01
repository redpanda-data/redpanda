/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/tee.hpp>

#include <iostream>
#include <sstream>

struct tee_wrapper {
    using device_t
      = boost::iostreams::tee_device<std::ostream, std::stringstream>;

    using stream_t = boost::iostreams::stream<device_t>;

    tee_wrapper()
      : sstr{}
      , device{std::cout, sstr}
      , stream{device} {}

    tee_wrapper(const tee_wrapper&) = delete;
    tee_wrapper& operator=(const tee_wrapper&) = delete;
    tee_wrapper(tee_wrapper&&) = delete;
    tee_wrapper& operator=(tee_wrapper&&) = delete;

    ~tee_wrapper() {
        if (stream.is_open()) {
            stream->flush();
            stream->close();
        }
    }

    std::string string() const { return sstr.str(); }

    std::stringstream sstr;
    device_t device;
    stream_t stream;
};
