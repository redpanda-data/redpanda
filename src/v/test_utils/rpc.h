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
#include "reflection/adl.h"
#include "reflection/async_adl.h"

#include <memory>

template<typename T>
T serialize_roundtrip_rpc(T&& t) {
    iobuf io = reflection::to_iobuf(std::forward<T>(t));
    iobuf_parser parser(std::move(io));
    return reflection::adl<T>{}.from(parser);
}
template<typename T>
ss::future<T> async_serialize_roundtrip_rpc(T&& t) {
    auto b = std::make_unique<iobuf>();
    auto raw = b.get();
    return reflection::async_adl<T>{}
      .to(*raw, std::move(t))
      .then([b = std::move(b)]() mutable {
          auto p = std::make_unique<iobuf_parser>(std::move(*b));
          auto raw = p.get();
          return reflection::async_adl<T>{}.from(*raw).finally(
            [p = std::move(p)] {});
      });
}
