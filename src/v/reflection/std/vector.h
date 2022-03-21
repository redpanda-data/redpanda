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

#pragma once
#include "reflection/async_adl.h"

#include <vector>

namespace reflection {

template<typename... Args>
struct async_adl<std::vector<Args...>>
  : public detail::async_adl_list<std::vector<Args...>> {};

} // namespace reflection
