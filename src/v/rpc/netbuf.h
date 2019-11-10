#pragma once

#include "bytes/iobuf.h"
#include "rpc/serialize.h"
#include "rpc/types.h"

#include <seastar/core/scattered_message.hh>

namespace rpc {
class netbuf {
public:
    netbuf() ;
    netbuf(netbuf&& o) noexcept = default;
    netbuf& operator=(netbuf&& o) noexcept = default;
    netbuf(const netbuf&) = delete;

    /// \brief used to send the bytes down the wire
    /// we re-compute the header-checksum on every call
    scattered_message<char> as_scattered() &&;

    void set_correlation_id(uint32_t);
    void set_service_method_id(uint32_t);

    template<typename T>
    void serialize_type(T&& t) {
        ::rpc::serialize(_out, std::forward<T>(t));
    }

private:
    size_t payload_size() const {
        return _out.size_bytes() - sizeof(_hdr);
    }
    header _hdr;
    iobuf _out;
    iobuf::placeholder _hdr_hldr;
};

} // namespace rpc
