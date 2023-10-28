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

#include "bytes/bytes.h"
#include "bytes/iobuf.h"
#include "outcome.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/bool_class.hh>

#include <boost/beast/http/fields.hpp>
#include <boost/beast/http/impl/fields.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace http {

using multipart_fields = boost::beast::http::fields;

/// A multipart writer adhering to RFC2046.
/// https://datatracker.ietf.org/doc/html/rfc2046
///
/// \note All methods must be called sequentially. That is, no method may be
/// invoked before the previous method's returned future is resolved.
/// Last call must be to close.
class multipart_writer {
public:
    /// \note The caller must ensure \p out outlives the writer object
    /// constructed here.
    explicit multipart_writer(
      ss::output_stream<char>& out, std::string boundary);
    multipart_writer(multipart_writer&&) = default;
    ~multipart_writer() = default;

    // No compelling reasons for these to exist.
    multipart_writer(const multipart_writer&) = delete;
    multipart_writer& operator=(const multipart_writer&) = delete;
    multipart_writer& operator=(multipart_writer&&) = delete;

    ss::future<>
    write_part(multipart_fields&& header, ss::temporary_buffer<char>&& body);
    ss::future<> write_part(multipart_fields&& header, iobuf&& body);
    ss::future<> close();

private:
    ss::future<> write_delimiter();
    ss::future<> write_part_header(multipart_fields&& header);

private:
    ss::output_stream<char>& _out;
    std::string _boundary;

    bool _first_part_sent = false;
    bool _closed = false;
};

} // namespace http
