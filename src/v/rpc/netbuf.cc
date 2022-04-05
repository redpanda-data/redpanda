// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "compression/stream_zstd.h"
#include "hashing/xx.h"
#include "reflection/adl.h"
#include "rpc/types.h"
#include "vassert.h"

namespace rpc {
iobuf header_as_iobuf(const header& h) {
    iobuf b;
    b.reserve_memory(size_of_rpc_header);
    reflection::adl<rpc::header>{}.to(b, h);
    vassert(
      b.size_bytes() == size_of_rpc_header,
      "Header size must be known and exact");
    return b;
}
/// \brief used to send the bytes down the wire
/// we re-compute the header-checksum on every call
ss::scattered_message<char> netbuf::as_scattered() && {
    if (_hdr.correlation_id == 0 || _hdr.meta == 0) {
        throw std::runtime_error(
          "cannot compose scattered view with incomplete header. missing "
          "correlation_id or remote method id");
    }
    if (
      _out.size_bytes() >= _min_compression_bytes
      && rpc::compression_type::zstd == _hdr.compression) {
        compression::stream_zstd fn;
        _out = fn.compress(std::move(_out));
    } else {
        // didn't meet min requirements
        _hdr.compression = rpc::compression_type::none;
    }
    incremental_xxhash64 h;
    auto in = iobuf::iterator_consumer(_out.cbegin(), _out.cend());
    in.consume(_out.size_bytes(), [&h](const char* src, size_t sz) {
        h.update(src, sz);
        return ss::stop_iteration::no;
    });
    _hdr.payload_checksum = h.digest();
    _hdr.payload_size = _out.size_bytes();
    _hdr.header_checksum = rpc::checksum_header_only(_hdr);
    _out.prepend(header_as_iobuf(_hdr));

    // prepare for output
    return iobuf_as_scattered(std::move(_out));
}

} // namespace rpc
