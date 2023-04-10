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

#include <seastar/util/log.hh>

#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/tee.hpp>

#include <iostream>
#include <sstream>

/*
 * A utility for capturing seastar::logger output.
 *
 * TODO: ideally this introspects the installed logger so that the proper logger
 * could be reinstalled in the destructor. The default logger for seastar is
 * std::cerr which is used here. As long as this utility is only used in unit
 * tests then this default behavior should be sufficient.
 */
struct tee_wrapper {
    using device_t
      = boost::iostreams::tee_device<std::ostream, std::stringstream>;

    using stream_t = boost::iostreams::stream<device_t>;

    explicit tee_wrapper(seastar::logger& logger)
      : sstr{}
      , device{std::cerr, sstr}
      , stream{device}
      , logger(logger) {
        logger.set_ostream(stream);
    }

    tee_wrapper(const tee_wrapper&) = delete;
    tee_wrapper& operator=(const tee_wrapper&) = delete;
    tee_wrapper(tee_wrapper&&) = delete;
    tee_wrapper& operator=(tee_wrapper&&) = delete;

    ~tee_wrapper() {
        logger.set_ostream(std::cerr);
        if (stream.is_open()) {
            stream->flush();
            stream->close();
        }
    }

    std::string string() const { return sstr.str(); }

    std::stringstream sstr;
    device_t device;
    stream_t stream;
    seastar::logger& logger;
};
