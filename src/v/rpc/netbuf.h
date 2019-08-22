#pragma once

#include "bytes/bytes_ostream.h"
#include "rpc/serialize.h"
#include "rpc/types.h"

#include <seastar/core/scattered_message.hh>

namespace rpc {
class netbuf {
public:
    netbuf();
    netbuf(netbuf&& o) noexcept;
    netbuf& operator=(netbuf&& o) noexcept;
    netbuf(const netbuf&) = delete;

    /// \brief used to send the bytes down the wire
    /// we re-compute the header-checksum on every call
    scattered_message<char> scattered_view();

    void set_correlation_id(uint32_t);
    void set_service_method_id(uint32_t);

    template<typename T>
    void serialize_type(T& t) {
        ::rpc::serialize(_out, t);
    }

    bytes_ostream&& release() && {
        return std::move(_out);
    }

private:
    size_t payload_size() const {
        return _out.size_bytes() - sizeof(_hdr);
    }
    header _hdr;
    bytes_ostream _out;
    char* _hdr_ptr = nullptr;
};

} // namespace rpc
