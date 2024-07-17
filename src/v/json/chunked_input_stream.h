// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "bytes/streambuf.h"
#include "json/encodings.h"
#include "json/istreamwrapper.h"

namespace json {

namespace impl {

/**
 * \brief An in-memory input stream with non-contiguous memory allocation.
 */
template<typename Encoding = ::json::UTF8<>>
class chunked_input_stream {
public:
    using Ch = Encoding::Ch;

    explicit chunked_input_stream(iobuf&& buf)
      : _buf(std::move(buf))
      , _is(_buf)
      , _sis{&_is}
      , _isw(_sis) {}

    /**
     * \defgroup Implement rapidjson::Stream
     */
    /**@{*/

    Ch Peek() const { return _isw.Peek(); }
    Ch Peek4() const { return _isw.Peek4(); }
    Ch Take() { return _isw.Take(); }
    size_t Tell() const { return _isw.Tell(); }
    void Put(Ch ch) { return _isw.Put(ch); }
    Ch* PutBegin() { return _isw.PutBegin(); }
    size_t PutEnd(Ch* ch) { return _isw.PutEnd(ch); }
    void Flush() { return _isw.Flush(); }

    /**@}*/

private:
    iobuf _buf;
    iobuf_istreambuf _is;
    std::istream _sis;
    ::json::IStreamWrapper _isw;
};

} // namespace impl

template<typename Encoding>
using generic_chunked_input_stream = impl::chunked_input_stream<Encoding>;

using chunked_input_stream = generic_chunked_input_stream<UTF8<>>;

} // namespace json
