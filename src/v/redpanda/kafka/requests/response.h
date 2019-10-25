#pragma once

#include "bytes/bytes_ostream.h"
#include "redpanda/kafka/requests/response_writer.h"
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

    const bytes_ostream& buf() const {
        return _buf;
    }

private:
    bytes_ostream _buf;
    response_writer _writer;
};

using response_ptr = foreign_ptr<std::unique_ptr<response>>;

} // namespace kafka
