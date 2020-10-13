#pragma once
#include "model/record_batch_reader.h"
#include "reflection/async_adl.h"

namespace reflection {

template<>
struct async_adl<model::record_batch_reader> {
    ss::future<> to(iobuf& out, model::record_batch_reader&&);

    ss::future<model::record_batch_reader> from(iobuf_parser&);
};

} // namespace reflection
