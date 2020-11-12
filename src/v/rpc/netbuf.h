/*
 * Copyright 2020 Vectorized, Inc.
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
#include "rpc/types.h"
#include "vassert.h"

#include <seastar/core/scattered_message.hh>

namespace rpc {
class netbuf {
public:
    /// \brief used to send the bytes down the wire
    /// we re-compute the header-checksum on every call
    ss::scattered_message<char> as_scattered() &&;

    void set_status(rpc::status);
    void set_correlation_id(uint32_t);
    void set_compression(rpc::compression_type c);
    void set_service_method_id(uint32_t);
    void set_min_compression_bytes(size_t);
    iobuf& buffer();

private:
    size_t _min_compression_bytes{1024};
    header _hdr;
    iobuf _out;
};

inline iobuf& netbuf::buffer() { return _out; }
inline void netbuf::set_compression(rpc::compression_type c) {
    vassert(
      c >= compression_type::min && c <= compression_type::max,
      "invalid compression type: {}",
      int(c));
    _hdr.compression = c;
}
inline void netbuf::set_status(rpc::status st) {
    _hdr.meta = std::underlying_type_t<rpc::status>(st);
}
inline void netbuf::set_correlation_id(uint32_t x) { _hdr.correlation_id = x; }
inline void netbuf::set_service_method_id(uint32_t x) { _hdr.meta = x; }
inline void netbuf::set_min_compression_bytes(size_t min) {
    _min_compression_bytes = min;
}

} // namespace rpc
