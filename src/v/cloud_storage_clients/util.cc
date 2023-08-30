/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/util.h"

#include "bytes/streambuf.h"
#include "net/connection.h"

#include <boost/property_tree/xml_parser.hpp>

namespace {

bool is_abort_or_gate_close_exception(const std::exception_ptr& ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const ss::abort_requested_exception&) {
        return true;
    } catch (const ss::gate_closed_exception&) {
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

namespace cloud_storage_clients::util {

bool has_abort_or_gate_close_exception(const ss::nested_exception& ex) {
    return is_abort_or_gate_close_exception(ex.inner)
           || is_abort_or_gate_close_exception(ex.outer);
}

error_outcome handle_client_transport_error(
  std::exception_ptr current_exception, ss::logger& logger) {
    auto outcome = error_outcome::retry;

    try {
        std::rethrow_exception(current_exception);
    } catch (const std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            vlog(logger.warn, "File removed during download: ", e.path1());
            outcome = error_outcome::retry;
        } else {
            vlog(logger.error, "Filesystem error {}", e);
            outcome = error_outcome::fail;
        }
    } catch (const std::system_error& cerr) {
        // The system_error is type erased and not convenient for selective
        // handling. The following errors should be retried:
        // - connection refused, timed out or reset by peer
        // - network temporary unavailable
        // Shouldn't be retried
        // - any filesystem error
        // - broken-pipe
        // - any other network error (no memory, bad socket, etc)
        if (net::is_reconnect_error(cerr)) {
            vlog(
              logger.warn,
              "System error susceptible for retry {}",
              cerr.what());

        } else {
            vlog(logger.error, "System error {}", cerr);
            outcome = error_outcome::fail;
        }
    } catch (const ss::timed_out_error& terr) {
        // This should happen when the connection pool was disconnected
        // from the S3 endpoint and subsequent connection attmpts failed.
        vlog(logger.warn, "Connection timeout {}", terr.what());
    } catch (const boost::system::system_error& err) {
        if (
          err.code() != boost::beast::http::error::short_read
          && err.code() != boost::beast::http::error::partial_message) {
            vlog(logger.warn, "Connection failed {}", err.what());
            outcome = error_outcome::fail;
        } else {
            // This is a short read error that can be caused by the abrupt TLS
            // shutdown. The content of the received buffer is discarded in this
            // case and http client receives an empty buffer.
            vlog(
              logger.info,
              "Server disconnected: '{}', retrying HTTP request",
              err.what());
        }
    } catch (const ss::gate_closed_exception&) {
        vlog(logger.debug, "Gate closed");
        throw;
    } catch (const ss::abort_requested_exception&) {
        vlog(logger.debug, "Abort requested");
        throw;
    } catch (const ss::nested_exception& ex) {
        if (has_abort_or_gate_close_exception(ex)) {
            vlog(logger.debug, "Nested abort or gate closed: {}", ex);
            throw;
        }

        vlog(logger.error, "Unexpected error {}", std::current_exception());
        outcome = error_outcome::fail;
    } catch (...) {
        vlog(logger.error, "Unexpected error {}", std::current_exception());
        outcome = error_outcome::fail;
    }

    return outcome;
}

ss::future<iobuf>
drain_response_stream(http::client::response_stream_ref resp) {
    const auto transfer_encoding = resp->get_headers().find(
      boost::beast::http::field::transfer_encoding);

    if (
      transfer_encoding != resp->get_headers().end()
      && transfer_encoding->value().find("chunked")) {
        return drain_chunked_response_stream(resp);
    }

    return ss::do_with(
      iobuf(), [resp = std::move(resp)](iobuf& outbuf) mutable {
          return ss::do_until(
                   [resp] { return resp->is_done(); },
                   [resp, &outbuf] {
                       return resp->recv_some().then([&outbuf](iobuf&& chunk) {
                           outbuf.append(std::move(chunk));
                       });
                   })
            .then([&outbuf] {
                return ss::make_ready_future<iobuf>(std::move(outbuf));
            });
      });
}

ss::future<iobuf>
drain_chunked_response_stream(http::client::response_stream_ref resp) {
    return ss::do_with(
      resp->as_input_stream(),
      iobuf(),
      [resp](ss::input_stream<char>& stream, iobuf& outbuf) mutable {
          return ss::do_until(
                   [&stream] { return stream.eof(); },
                   [&stream, &outbuf] {
                       return stream.read().then(
                         [&outbuf](ss::temporary_buffer<char>&& chunk) {
                             outbuf.append(std::move(chunk));
                         });
                   })
            .then([&outbuf] {
                return ss::make_ready_future<iobuf>(std::move(outbuf));
            });
      });
}

boost::property_tree::ptree iobuf_to_ptree(iobuf&& buf, ss::logger& logger) {
    namespace pt = boost::property_tree;
    try {
        iobuf_istreambuf strbuf(buf);
        std::istream stream(&strbuf);
        pt::ptree res;
        pt::read_xml(stream, res);
        return res;
    } catch (...) {
        log_buffer_with_rate_limiting("unexpected reply", buf, logger);
        vlog(logger.error, "!!parsing error {}", std::current_exception());
        throw;
    }
}

std::chrono::system_clock::time_point parse_timestamp(std::string_view sv) {
    std::tm tm = {};
    std::stringstream ss({sv.data(), sv.size()});
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S.Z%Z");
    return std::chrono::system_clock::from_time_t(timegm(&tm));
}

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

std::vector<object_key> all_paths_to_file(const object_key& path) {
    if (!path().has_filename()) {
        return {};
    }

    std::vector<object_key> paths;
    std::filesystem::path current_path;
    for (auto path_iter = path().begin(); path_iter != path().end();
         ++path_iter) {
        if (current_path == "") {
            current_path += *path_iter;
        } else {
            current_path /= *path_iter;
        }

        paths.emplace_back(current_path);
    }

    return paths;
}

} // namespace cloud_storage_clients::util
