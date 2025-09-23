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

#include <type_traits>
#include <variant>

namespace detail {
template<
  template<typename... MappedOption> typename TypeCombiner,
  template<typename OrigType> typename TypeMapper,
  typename... Option>
static constexpr std::type_identity<TypeCombiner<TypeMapper<Option>...>>
map_and_reassemble_variant_impl(std::type_identity<std::variant<Option...>>) {
    return {};
}
} // namespace detail

template<
  typename Variant,
  template<typename... MappedOption> typename TypeCombiner,
  template<typename OrigType> typename TypeMapper>
using map_and_reassemble_variant
  = decltype(detail::map_and_reassemble_variant_impl<TypeCombiner, TypeMapper>(
    std::type_identity<Variant>{}))::type;

template<typename Variant>
using variant_of_identities
  = map_and_reassemble_variant<Variant, std::variant, std::type_identity>;

template<typename Variant>
using tuple_of_identities
  = map_and_reassemble_variant<Variant, std::tuple, std::type_identity>;
