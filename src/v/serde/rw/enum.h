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
#include "serde/rw/fixed.h"
#include "serde/rw/rw.h"
#include "serde/serde_exception.h"
#include "serde/serde_is_enum.h"
#include "serde/type_str.h"
#include "ssx/sformat.h"

#include <limits>
#include <utility>

namespace serde {

template<SerdeWriteOutput Output, typename T>
requires(serde_is_enum_v<std::decay_t<T>>)
void tag_invoke(tag_t<write_tag>, Output& out, T t) {
    static_assert(std::is_scoped_enum_v<std::decay_t<T>>);
    using Type = std::decay_t<T>;
    detail::validate_serde_enum(t);
    const auto val = static_cast<std::underlying_type_t<Type>>(t);
    write(out, static_cast<serde_enum_serialized_t>(val));
}

template<typename T>
requires serde_is_enum_v<std::decay_t<T>>
void tag_invoke(
  tag_t<read_tag>, iobuf_parser& in, T& t, const std::size_t bytes_left_limit) {
#if defined(__cpp_lib_is_scoped_enum) && __cpp_lib_is_scoped_enum >= 202011L
    static_assert(std::is_scoped_enum_v<std::decay_t<T>>);
#endif
    using Type = std::decay_t<T>;

    const auto val = read_nested<serde_enum_serialized_t>(in, bytes_left_limit);
    if (
      unlikely(
        std::cmp_greater(
          val, std::numeric_limits<std::underlying_type_t<Type>>::max()))) {
        throw serde_exception(fmt_with_ctx(
          ssx::sformat,
          "enum value {} too large for {}",
          val,
          type_str<Type>()));
    }
    t = static_cast<Type>(val);
}

} // namespace serde
