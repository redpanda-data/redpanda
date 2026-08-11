/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/xml.h"

#include "base/vlog.h"
#include "bytes/streambuf.h"

#include <boost/property_tree/xml_parser.hpp>

#include <exception>

namespace {
void log_buffer_with_rate_limiting(
  const char* msg, iobuf& buf, ss::logger& logger) {
    static constexpr int buffer_size = 0x100;
    static constexpr auto rate_limit = std::chrono::seconds(1);
    thread_local static ss::logger::rate_limit rate(rate_limit);
    auto log_with_rate_limit = [&logger](
                                 ss::logger::format_info fmt, auto... args) {
        logger.log(ss::log_level::warn, rate, fmt, args...);
    };
    iobuf_istreambuf strbuf(buf);
    std::istream stream(&strbuf);
    std::array<char, buffer_size> str{};
    auto sz = stream.readsome(str.data(), buffer_size);
    auto sview = std::string_view(str.data(), sz);
    vlog(log_with_rate_limit, "{}: {}", msg, sview);
}

} // namespace

namespace xml {

boost::property_tree::ptree
iobuf_to_ptree(iobuf&& buf, ss::logger& logger, log_body_on_failure log_body) {
    namespace pt = boost::property_tree;
    try {
        iobuf_istreambuf strbuf(buf);
        std::istream stream(&strbuf);
        pt::ptree res;
        pt::read_xml(stream, res);
        return res;
    } catch (...) {
        if (log_body) {
            log_buffer_with_rate_limiting("unexpected reply", buf, logger);
        }
        vlog(logger.error, "!!parsing error {}", std::current_exception());
        throw;
    }
}

} // namespace xml
