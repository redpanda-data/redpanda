/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"

#include <streambuf>

/// A simple ostream buffer appender. No other op is currently supported.
/// Currently works with char-by-char iterators as well. See iobuf_tests.cc
///
/// iobuf underlying;
/// iobuf_ostreambuf obuf(underlying);
/// std::ostream os(&obuf);
///
/// os << "hello world";
///
class iobuf_ostreambuf final : public std::streambuf {
public:
    explicit iobuf_ostreambuf(iobuf& o) noexcept
      : _buf(&o) {}
    iobuf_ostreambuf(iobuf_ostreambuf&&) noexcept = default;
    iobuf_ostreambuf& operator=(iobuf_ostreambuf&&) noexcept = default;
    iobuf_ostreambuf(const iobuf_ostreambuf&) = delete;
    iobuf_ostreambuf& operator=(const iobuf_ostreambuf&) = delete;
    int_type overflow(int_type c = traits_type::eof()) final {
        if (c == traits_type::eof()) {
            return traits_type::eof();
        }
        char_type ch = traits_type::to_char_type(c);
        return xsputn(&ch, 1) == 1 ? c : traits_type::eof();
    }
    std::streamsize xsputn(const char_type* s, std::streamsize n) final {
        _buf->append(s, n);
        return n;
    }
    ~iobuf_ostreambuf() noexcept override = default;

private:
    iobuf* _buf;
};
