#pragma once

#include "rpc/client.h"
#include "rpc/netbuf.h"
#include "rpc/parse_utils.h"
#include "rpc/service.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "utils/fragbuf.h"

namespace demo {
struct simple_request {
    fragbuf buf;
};
struct simple_reply {
    fragbuf buf;
};
struct simple_service final : public rpc::service {
    struct client;
    scheduling_group& get_scheduling_group() override {
        throw std::runtime_error("no sc in simple_service");
    }
    smp_service_group& get_smp_service_group() override {
        throw std::runtime_error("no ssg in simple_service");
    }
    rpc::method* method_from_id(uint32_t idx) final {
        if (idx == 42) {
            return &fourty_two_method;
        }
        return nullptr;
    }
    future<simple_reply>
    fourty_two(demo::simple_request simple, rpc::streaming_context&) {
        return make_ready_future<simple_reply>(
          simple_reply{std::move(simple.buf)});
    }
    /// \brief san_francisco -> mount_tamalpais
    inline future<rpc::netbuf>
    raw_fourty_two(input_stream<char>& in, rpc::streaming_context& ctx) {
        auto fapply = execution_helper<simple_request, simple_reply>();
        // clang-format off
        return fapply.exec(in, ctx, 42, [this](
                 simple_request r, rpc::streaming_context& ctx) {
                 return fourty_two(std::move(r), ctx);
        });
        // clang-format on
    }
    rpc::method fourty_two_method{
      [this](input_stream<char>& in, rpc::streaming_context& ctx) {
          return raw_fourty_two(in, ctx);
      }};
};
struct simple_service::client final : public rpc::client {
    client(rpc::client_configuration c)
      : rpc::client(std::move(c)) {
    }
    inline future<rpc::client_context<simple_reply>>
    fourty_two(simple_request r) {
        return send_typed<simple_request, simple_reply>(std::move(r), 42);
    }
};
} // namespace demo
