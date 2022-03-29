/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "script_context_router.h"

#include "reflection/absl/node_hash_map.h"

namespace reflection {

ss::future<>
async_adl<coproc::read_context>::to(iobuf& out, coproc::read_context&& rctx) {
    reflection::serialize(
      out,
      coproc::read_context::version,
      rctx.absolute_start,
      rctx.last_read,
      rctx.last_acked);
    return ss::now();
}

ss::future<coproc::read_context>
async_adl<coproc::read_context>::from(iobuf_parser& in) {
    coproc::read_context rctx;
    auto version = adl<int8_t>{}.from(in);
    vassert(
      version == coproc::read_context::version,
      "Version {} is not supported. Only supporting versions up to {}",
      rctx.version,
      coproc::read_context::version);
    rctx.absolute_start = adl<model::offset>{}.from(in);
    rctx.last_read = adl<model::offset>{}.from(in);
    rctx.last_acked = adl<model::offset>{}.from(in);
    return ss::make_ready_future<coproc::read_context>(std::move(rctx));
}

ss::future<>
async_adl<coproc::write_context>::to(iobuf& out, coproc::write_context&& wctx) {
    reflection::serialize(out, coproc::write_context::version);
    return async_adl<coproc::write_context::offsets_t>{}.to(
      out, std::move(wctx.offsets));
}

ss::future<coproc::write_context>
async_adl<coproc::write_context>::from(iobuf_parser& in) {
    int8_t version = adl<int8_t>{}.from(in);
    vassert(
      version == coproc::write_context::version,
      "Version {} is not supported. Only supporting versions up to {}",
      version,
      coproc::write_context::version);
    return async_adl<coproc::write_context::offsets_t>{}.from(in).then(
      [](coproc::write_context::offsets_t result) {
          return coproc::write_context{.offsets = std::move(result)};
      });
}

ss::future<> async_adl<ss::lw_shared_ptr<coproc::source>>::to(
  iobuf& out, ss::lw_shared_ptr<coproc::source>&& src) {
    /// Take copies of the data the shared_ptr points to
    auto rctx_cp = src->rctx;
    auto wctx_cp = src->wctx;
    return async_adl<coproc::read_context>{}
      .to(out, std::move(rctx_cp))
      .then([&out, wctx_cp = std::move(wctx_cp)]() mutable {
          return async_adl<coproc::write_context>{}.to(out, std::move(wctx_cp));
      });
}

ss::future<ss::lw_shared_ptr<coproc::source>>
async_adl<ss::lw_shared_ptr<coproc::source>>::from(iobuf_parser& in) {
    return async_adl<coproc::read_context>{}.from(in).then(
      [&in](coproc::read_context rctx) {
          return async_adl<coproc::write_context>{}.from(in).then(
            [rctx = std::move(rctx)](coproc::write_context wctx) mutable {
                auto src = ss::make_lw_shared<coproc::source>();
                src->rctx = std::move(rctx);
                src->wctx = std::move(wctx);
                return src;
            });
      });
}

} // namespace reflection
