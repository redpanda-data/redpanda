#pragma once

#include "rpc/types.h"
#include "rpc/parse_utils.h"

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
    virtual scheduling_group& get_scheduling_group() = 0;
    virtual smp_service_group& get_smp_service_group() = 0;
    /// \brief return nullptr when method not found
    virtual method* method_from_id(uint32_t) = 0;
};

template<typename Input, typename Output>
struct service::execution_helper {
    using input = Input;
    using output = Output;

    template<typename Func>
    inline future<netbuf> exec(
      input_stream<char>& in,
      streaming_context& ctx,
      uint32_t method_id,
      Func&& f) {
        // clang-format off
        return ctx.reserve_memory(ctx.get_header().size)
          .then([f = std::forward<Func>(f), method_id, &in, &ctx] (
                 semaphore_units<> u) mutable {
              return parse_type<Input>(in, ctx.get_header())
                .then([f = std::forward<Func>(f), method_id,
                       &in, &ctx, u = std::move(u)](Input t) mutable {
                    ctx.signal_body_parse();
                    return f(std::move(t), ctx);
                })
                .then([u = std::move(u), method_id](Output out) mutable {
                    auto b = netbuf();
                    b.serialize_type(std::move(out));
                    b.set_service_method_id(method_id);
                    return make_ready_future<netbuf>(std::move(b));
                });
          });
        // clang-format on
    }
};
} // namespace rpc
