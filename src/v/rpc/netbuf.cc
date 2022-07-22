// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "bytes/scattered_message.h"
#include "compression/async_stream_zstd.h"
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
ss::future<ss::scattered_message<char>> netbuf::as_scattered() && {
    // Move object members into coroutine before first supension.
    iobuf out_buf = std::move(_out);
    auto hdr = std::move(_hdr);

    if (hdr.correlation_id == 0 || hdr.meta == 0) {
        throw std::runtime_error(
          "cannot compose scattered view with incomplete header. missing "
          "correlation_id or remote method id");
    }
    if (
      out_buf.size_bytes() >= _min_compression_bytes
      && rpc::compression_type::zstd == hdr.compression) {
        auto& zstd_inst = compression::async_stream_zstd_instance();
        out_buf = co_await zstd_inst.compress(std::move(out_buf));
    } else {
        // didn't meet min requirements
        hdr.compression = rpc::compression_type::none;
    }
    incremental_xxhash64 h;
    auto in = iobuf::iterator_consumer(out_buf.cbegin(), out_buf.cend());
    in.consume(out_buf.size_bytes(), [&h](const char* src, size_t sz) {
        h.update(src, sz);
        return ss::stop_iteration::no;
    });
    hdr.payload_checksum = h.digest();
    hdr.payload_size = out_buf.size_bytes();
    hdr.header_checksum = rpc::checksum_header_only(hdr);
    out_buf.prepend(header_as_iobuf(hdr));

    // prepare for output
    co_return iobuf_as_scattered(std::move(out_buf));
}

} // namespace rpc
