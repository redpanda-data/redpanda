/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "reflection/async_adl.h"
#include "rpc/parse_utils.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "ssx/sformat.h"

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
    virtual void setup_metrics() = 0;
};

class rpc_internal_body_parsing_exception : public std::exception {
public:
    explicit rpc_internal_body_parsing_exception(const std::exception_ptr& e)
      : _what(
        ssx::sformat("Unable to parse received RPC request payload - {}", e)) {}

    const char* what() const noexcept final { return _what.c_str(); }

private:
    seastar::sstring _what;
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
                .then_wrapped([f = std::forward<Func>(f),
                               &ctx](ss::future<Input> input_f) mutable {
                    if (input_f.failed()) {
                        throw rpc_internal_body_parsing_exception(
                          input_f.get_exception());
                    }
                    ctx.signal_body_parse();
                    auto input = input_f.get0();
                    return f(std::move(input), ctx);
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
