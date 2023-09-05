// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include <type_traits>

#pragma once

namespace serde {

using serde_enum_serialized_t = int32_t;

template<typename T>
inline constexpr bool serde_is_enum_v =
#if __has_cpp_attribute(__cpp_lib_is_scoped_enum)
  std::is_scoped_enum_v<T>
  && sizeof(std::decay_t<T>) <= sizeof(serde_enum_serialized_t);
#else
  std::is_enum_v<T>
  && sizeof(std::decay_t<T>) <= sizeof(serde_enum_serialized_t);
#endif

} // namespace serde
