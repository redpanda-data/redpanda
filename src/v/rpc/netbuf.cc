#include "rpc/netbuf.h"

#include "hashing/xx.h"

namespace rpc {

netbuf::netbuf() {
    _hdr_hldr = _out.reserve(sizeof(_hdr));
}
/// \brief used to send the bytes down the wire
/// we re-compute the header-checksum on every call
scattered_message<char> netbuf::as_scattered() && {
    constexpr const size_t size_header = sizeof(header);
    if (_hdr.correlation_id == 0 || _hdr.meta == 0) {
        throw std::runtime_error(
          "cannot compose scattered view with incomplete header. missing "
          "correlation_id or remote method id");
    }
    size_t i = 0;
    incremental_xxhash64 h;
    scattered_message<char> msg;
    for (const auto& buf : _out) {
        if (i++ == 0) {
            h.update(buf.get() + size_header, buf.size() - size_header);
        } else {
            h.update(buf.get(), buf.size());
        }
        msg.append_static(buf.get(), buf.size());
    }
    _hdr.checksum = h.digest();
    _hdr.size = _out.size_bytes() - size_header;
    // update the header
    _hdr_hldr.write(reinterpret_cast<const char*>(&_hdr), size_header);
    msg.on_delete([b = std::move(_out)] {});
    return msg;
}
void netbuf::set_correlation_id(uint32_t x) {
    _hdr.correlation_id = x;
}
void netbuf::set_service_method_id(uint32_t x) {
    _hdr.meta = x;
}

} // namespace rpc
