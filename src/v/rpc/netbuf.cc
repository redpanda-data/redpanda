#include "rpc/netbuf.h"

#include "bytes/iobuf.h"
#include "hashing/xx.h"
#include "reflection/adl.h"
#include "rpc/types.h"
#include "vassert.h"

namespace rpc {
static iobuf header_as_iobuf(const header& h) {
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

void netbuf::set_compression(rpc::compression_type c) { _hdr.compression = c; }
void netbuf::set_correlation_id(uint32_t x) { _hdr.correlation_id = x; }
void netbuf::set_service_method_id(uint32_t x) { _hdr.meta = x; }

} // namespace rpc
