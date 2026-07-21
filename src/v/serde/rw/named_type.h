// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "serde/rw/fixed.h"
#include "serde/rw/rw.h"
#include "utils/named_type.h"

namespace serde {

namespace detail {

template<typename T, typename Tag, typename IsConstexpr>
requires(
  fixed_serde_v<T>
  && !disable_fixed_serde_v<::detail::base_named_type<T, Tag, IsConstexpr>>
  && !has_nonmember_write_nested<
     ::detail::base_named_type<T, Tag, IsConstexpr>>)
struct fixed_serde_traits<::detail::base_named_type<T, Tag, IsConstexpr>> {
    using type = ::detail::base_named_type<T, Tag, IsConstexpr>;

    static constexpr bool supported = true;
    static constexpr size_t size = fixed_serde_size_v<T>;
    static constexpr bool requires_validation
      = fixed_serde_traits<T>::requires_validation;

    static void validate(const type& value)
    requires requires_validation
    {
        fixed_serde_traits<T>::validate(value());
    }
};

} // namespace detail

template<typename T, typename Tag, typename IsConstexpr>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  ::detail::base_named_type<T, Tag, IsConstexpr>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::decay_t<decltype(t)>;
    t = Type{read_nested<typename Type::type>(in, bytes_left_limit)};
}

template<
  SerdeWriteOutput Output,
  typename T,
  typename Tag,
  typename IsConstexpr>
void tag_invoke(
  tag_t<write_tag>,
  Output& out,
  ::detail::base_named_type<T, Tag, IsConstexpr> t) {
    return write(out, static_cast<T>(t));
}

} // namespace serde
