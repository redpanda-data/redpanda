// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/vlog.h"
#include "bytes/iobuf_parser.h"
#include "serde/checksum_t.h"
#include "serde/envelope.h"
#include "serde/rw/scalar.h"
#include "serde/serde_size_t.h"
#include "serde/type_str.h"
#include "serde_exception.h"
#include "ssx/sformat.h"

namespace serde {

struct header {
    version_t _version, _compat_version;
    size_t _bytes_left_limit;
    checksum_t _checksum;
};

template<typename T>
header read_header(iobuf_parser& in, const std::size_t bytes_left_limit) {
    using Type = std::decay_t<T>;

    version_t version;
    version_t compat_version;
    serde_size_t size;

    read_tag(in, version, bytes_left_limit);
    read_tag(in, compat_version, bytes_left_limit);
    read_tag(in, size, bytes_left_limit);

    auto checksum = checksum_t{};
    if constexpr (is_checksum_envelope<T>) {
        read_tag(in, checksum, bytes_left_limit);
    }

    if (unlikely(in.bytes_left() < size)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "bytes_left={}, size={}",
          in.bytes_left(),
          static_cast<int>(size)));
    }

    if (unlikely(in.bytes_left() - size < bytes_left_limit)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "envelope does not fit into bytes left: bytes_left={}, size={}, "
          "bytes_left_limit={}",
          in.bytes_left(),
          static_cast<int>(size),
          bytes_left_limit));
    }

    if (unlikely(compat_version > Type::redpanda_serde_version)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "read {}: compat_version={} > {}::version={}",
          type_str<Type>(),
          static_cast<int>(compat_version),
          type_str<T>(),
          static_cast<int>(Type::redpanda_serde_version)));
    }

    if (unlikely(version < Type::redpanda_serde_compat_version)) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "read {}: version={} < {}::compat_version={}",
          type_str<Type>(),
          static_cast<int>(version),
          type_str<T>(),
          static_cast<int>(Type::redpanda_serde_compat_version)));
    }

    return header{
      ._version = version,
      ._compat_version = compat_version,
      ._bytes_left_limit = in.bytes_left() - size,
      ._checksum = checksum};
}

} // namespace serde
