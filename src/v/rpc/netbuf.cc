#include "rpc/netbuf.h"

#include "bytes/iobuf.h"
#include "hashing/xx.h"

namespace rpc {

/// \brief used to send the bytes down the wire
/// we re-compute the header-checksum on every call
ss::scattered_message<char> netbuf::as_scattered() && {
    constexpr const size_t size_header = sizeof(header);
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
    _hdr.checksum = h.digest();
    _hdr.size = _out.size_bytes();
    // update the header
    ss::temporary_buffer<char> hdr_payload(
      reinterpret_cast<const char*>(&_hdr), size_header);
    _out.prepend(std::move(hdr_payload));

    // prepare for output
    return iobuf_as_scattered(std::move(_out));
}
void netbuf::set_correlation_id(uint32_t x) { _hdr.correlation_id = x; }
void netbuf::set_service_method_id(uint32_t x) { _hdr.meta = x; }

} // namespace rpc
