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

///\brief Wrap a std::istream around an iobuf
///
/// iobuf buf;
/// iobuf_istream is(std::move(buf));
/// std::string out;
/// is.istream() >> out;
class iobuf_istream {
public:
    explicit iobuf_istream(iobuf buf)
      : _buf(std::move(buf))
      , _isb(_buf)
      , _sis{&_isb} {}
    std::istream& istream() { return _sis; }

private:
    iobuf _buf;
    iobuf_istreambuf _isb;
    std::istream _sis;
};

///\brief Wrap a std::ostream around an iobuf
///
/// iobuf_ostream os;
/// os.ostream() << "Hello World";
/// iobuf buf = std::move(os).buf();
class iobuf_ostream {
public:
    iobuf_ostream()
      : _buf()
      , _osb(_buf)
      , _sos{&_osb} {}
    std::ostream& ostream() { return _sos; }
    iobuf buf() && { return std::move(_buf); }

private:
    iobuf _buf;
    iobuf_ostreambuf _osb;
    std::ostream _sos;
};
