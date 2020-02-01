#pragma once
#include "reflection/adl.h"
namespace reflection {
template<typename T>
struct async_adl {
    using type = std::remove_reference_t<std::decay_t<T>>;
    // provide extension point for rpc on the cases that we need to
    ss::future<> to(iobuf& out, type t) {
        reflection::adl<type>{}.to(out, std::move(t));
        return ss::make_ready_future<>();
    }
    ss::future<type> from(iobuf_parser& p) {
        return ss::make_ready_future<type>(reflection::adl<type>{}.from(p));
    }
};
} // namespace reflection
