/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "iceberg/values.h"

#include <gmock/gmock.h>

namespace iceberg::testing {

using namespace ::testing;

template<typename ValueT>
auto IcebergPrimitive(const auto& value) {
    return VariantWith<primitive_value>(
      VariantWith<ValueT>(Field(&ValueT::val, Eq(value))));
}

template<typename ValueT>
auto OptionalIcebergPrimitive(const auto& value) {
    return Optional(IcebergPrimitive<ValueT>(value));
}

template<typename... MatcherT>
auto IcebergStruct(MatcherT... matchers) {
    using struct_t = std::unique_ptr<struct_value>;
    return Optional(VariantWith<struct_t>(Pointee(Field(
      &struct_value::fields,
      ElementsAre(std::forward<MatcherT>(matchers)...)))));
}

template<typename MatcherT>
auto IcebergList(MatcherT matcher) {
    return Optional(VariantWith<std::unique_ptr<list_value>>(
      Pointee(Field(&list_value::elements, std::forward<MatcherT>(matcher)))));
}

template<typename MatcherT>
auto IcebergMap(MatcherT matcher) {
    return Optional(VariantWith<std::unique_ptr<map_value>>(
      Pointee(Field(&map_value::kvs, std::forward<MatcherT>(matcher)))));
}

template<typename KeyMatcherT, typename ValueMatcherT>
auto IcebergKeyValue(KeyMatcherT k_matcher, ValueMatcherT v_matcher) {
    return FieldsAre(k_matcher, v_matcher);
}

}; // namespace iceberg::testing
