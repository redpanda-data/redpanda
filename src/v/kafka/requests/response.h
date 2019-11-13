#pragma once

#include "bytes/iobuf.h"
#include "kafka/requests/response_writer.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

#include <memory>

namespace kafka {

class response {
public:
    response() noexcept
      : _writer(_buf) {
    }

    response_writer& writer() {
        return _writer;
    }

    const iobuf& buf() const {
        return _buf;
    }

    iobuf release() && {
        return std::move(_buf);
    }

private:
    iobuf _buf;
    response_writer _writer;
};

using response_ptr = foreign_ptr<std::unique_ptr<response>>;

} // namespace kafka
