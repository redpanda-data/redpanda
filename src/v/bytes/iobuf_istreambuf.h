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

/// A simple istream buffer reader.
///
/// iobuf underlying;
/// std::string out;
/// iobuf_istreambuf ibuf(underlying);
/// std::istream is(&ibuf);
///
/// is >> out;
///
class iobuf_istreambuf final : public std::streambuf {
public:
    explicit iobuf_istreambuf(iobuf& buf) noexcept
      : _curr(buf.begin())
      , _end(buf.end()) {
        std::ignore = underflow();
    }
    iobuf_istreambuf(iobuf_istreambuf&&) noexcept = default;
    iobuf_istreambuf& operator=(iobuf_istreambuf&&) noexcept = default;
    iobuf_istreambuf(const iobuf_istreambuf&) = delete;
    iobuf_istreambuf& operator=(const iobuf_istreambuf&) = delete;
    ~iobuf_istreambuf() noexcept override = default;

    int_type underflow() override {
        if (gptr() == egptr()) {
            while (_curr != _end) {
                if (_curr->is_empty()) {
                    _curr++;
                    continue;
                }
                setg(
                  _curr->get_write(), _curr->get_write(), _curr->get_current());
                _curr++;
                return traits_type::to_int_type(*gptr());
            }
        }
        return traits_type::eof();
    }

private:
    iobuf::iterator _curr;
    iobuf::iterator _end;
};
