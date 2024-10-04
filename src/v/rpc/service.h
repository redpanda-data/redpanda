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

#include "base/seastarx.h"
#include "net/types.h"
#include "reflection/async_adl.h"
#include "rpc/parse_utils.h"
#include "rpc/types.h"
#include "ssx/sformat.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>

#include <cstdint>

namespace rpc {

/// \brief most service implementations will be codegenerated
struct service {
    template<typename Input, typename Output, typename Codec>
    struct execution_helper;

    service() = default;
    virtual ~service() noexcept = default;
    virtual ss::scheduling_group& get_scheduling_group() = 0;
    virtual ss::smp_service_group& get_smp_service_group() = 0;
    /// \brief return nullptr when method not found
    virtual method* method_from_id(uint32_t) = 0;
    virtual void setup_metrics() = 0;
};

class rpc_internal_body_parsing_exception : public net::parsing_exception {
public:
    explicit rpc_internal_body_parsing_exception(const std::exception_ptr& e)
      : _what(ssx::sformat(
          "Unable to parse received RPC request payload - {}", e)) {}

    const char* what() const noexcept final { return _what.c_str(); }

private:
    seastar::sstring _what;
};

template<typename Input, typename Output, typename Codec>
struct service::execution_helper {
    using input = Input;
    using output = Output;

    /*
     * ensure that request and response types are compatible. this solves a
     * particular class of issue where someone marks for example the request as
     * exempt but forgets to mark the response. in this scenario it is then
     * possible for the assertion regarding unexpected encodings to fire.
     */
    static_assert(is_rpc_adl_exempt<Input> == is_rpc_adl_exempt<Output>);
    static_assert(is_rpc_serde_exempt<Input> == is_rpc_serde_exempt<Output>);

    /*
     * provided that input/output are no exempt from serde support, require that
     * they are both serde envelopes. this prevents a class of situation in
     * which a request/response is a natively supported type, but not actually
     * an envelope, such as a request being a raw integer or a named_type. we
     * want our rpc's to be versioned.
     */
    static_assert(is_rpc_serde_exempt<Input> || serde::is_envelope<Input>);
    static_assert(is_rpc_serde_exempt<Output> || serde::is_envelope<Output>);

    template<typename Func>
    static ss::future<netbuf> exec(
      ss::input_stream<char>& in,
      streaming_context& ctx,
      method_info method,
      Func&& f) {
        return ctx.permanent_memory_reservation(ctx.get_header().payload_size)
          .handle_exception([&ctx](const std::exception_ptr& e) {
              // It's possible to stop all waiters on a semaphore externally
              // with the semaphore's `broken` method. In which case
              // `permanent_memory_reservation` will return an exception.
              // We intercept it here to avoid a broken promise.
              ctx.body_parse_exception(e);
              return ss::make_exception_future(e);
          })
          .then([f = std::forward<Func>(f), method, &in, &ctx]() mutable {
              return parse_type<Input, Codec>(in, ctx.get_header())
                .then_wrapped([f = std::forward<Func>(f),
                               &ctx](ss::future<Input> input_f) mutable {
                    if (input_f.failed()) {
                        throw rpc_internal_body_parsing_exception(
                          input_f.get_exception());
                    }
                    ctx.signal_body_parse();
                    auto input = input_f.get();
                    return f(std::move(input), ctx);
                })
                .then([method, &ctx](Output out) mutable {
                    const auto version = Codec::response_version(
                      ctx.get_header());
                    auto b = std::make_unique<netbuf>();
                    auto raw_b = b.get();
                    raw_b->set_service_method(method);
                    raw_b->set_version(version);
                    return Codec::encode(
                             raw_b->buffer(), std::move(out), version)
                      .then([version, b = std::move(b)](
                              transport_version effective_version) {
                          /*
                           * this assertion is safe because the conditions under
                           * which this assertion would fail should have been
                           * verified in parse_type above.
                           */
                          vassert(
                            effective_version == version,
                            "Unexpected encoding at effective {} != {}. Input "
                            "{} Output {}",
                            effective_version,
                            version,
                            serde::type_str<Input>(),
                            serde::type_str<Output>());
                          return std::move(*b);
                      });
                });
          });
    }
};
} // namespace rpc
