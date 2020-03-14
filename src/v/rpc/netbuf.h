#pragma once

#include "bytes/iobuf.h"
#include "rpc/types.h"

#include <seastar/core/scattered_message.hh>

namespace rpc {
class netbuf {
public:
    /// \brief used to send the bytes down the wire
    /// we re-compute the header-checksum on every call
    ss::scattered_message<char> as_scattered() &&;

    void set_correlation_id(uint32_t);
    void set_compression(rpc::compression_type c);
    void set_service_method_id(uint32_t);

    iobuf& buffer() { return _out; }

private:
    header _hdr;
    iobuf _out;
};

} // namespace rpc
