/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <variant>

namespace util::detail {

template<typename... Option>
static constexpr std::variant<std::type_identity<Option>...>
variant_of_tags_impl(std::type_identity<std::variant<Option...>>) {
    return {};
}
template<typename... Option>
static constexpr std::tuple<std::type_identity<Option>...>
tuple_of_tags_impl(std::type_identity<std::variant<Option...>>) {
    return {};
}

} // namespace util::detail

template<typename Variant>
using variant_of_identities = decltype(util::detail::variant_of_tags_impl(
  std::type_identity<Variant>{}));

template<typename Variant>
using tuple_of_identities = decltype(util::detail::tuple_of_tags_impl(
  std::type_identity<Variant>{}));

namespace util::detail {

template<typename Variant, typename... Extra>
struct extend_variant;

template<typename... Ts, typename... Extra>
struct extend_variant<std::variant<Ts...>, Extra...> {
    using type = std::variant<Ts..., Extra...>;
};

} // namespace util::detail

template<typename Variant, typename... Extra>
using extend_variant_t =
  typename util::detail::extend_variant<Variant, Extra...>::type;
