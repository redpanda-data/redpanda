#pragma once

#include "reflection/async_adl.h"
#include "rpc/netbuf.h"
#include "rpc/parse_utils.h"
#include "rpc/types.h"
#include "seastarx.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>

#include <cstdint>

namespace rpc {

/// \brief most service implementations will be codegenerated
struct service {
    template<typename Input, typename Output>
    struct execution_helper;

    service() = default;
    virtual ~service() noexcept = default;
    virtual ss::scheduling_group& get_scheduling_group() = 0;
    virtual ss::smp_service_group& get_smp_service_group() = 0;
    /// \brief return nullptr when method not found
    virtual method* method_from_id(uint32_t) = 0;
};

template<typename Input, typename Output>
struct service::execution_helper {
    using input = Input;
    using output = Output;

    template<typename Func>
    static ss::future<netbuf> exec(
      ss::input_stream<char>& in,
      streaming_context& ctx,
      uint32_t method_id,
      Func&& f) {
        return ctx.permanent_memory_reservation(ctx.get_header().payload_size)
          .then([f = std::forward<Func>(f), method_id, &in, &ctx]() mutable {
              return parse_type<Input>(in, ctx.get_header())
                .then([f = std::forward<Func>(f), &ctx](Input t) mutable {
                    ctx.signal_body_parse();
                    return f(std::move(t), ctx);
                })
                .then([method_id](Output out) mutable {
                    auto b = std::make_unique<netbuf>();
                    auto raw_b = b.get();
                    raw_b->set_service_method_id(method_id);
                    return reflection::async_adl<Output>{}
                      .to(raw_b->buffer(), std::move(out))
                      .then([b = std::move(b)] { return std::move(*b); });
                });
          });
    }
};
} // namespace rpc
