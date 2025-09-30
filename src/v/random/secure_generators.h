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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

// Random generators useful for testing.
namespace random_generators {

template<typename T>
T get_int_secure(bool use_private);

ss::sstring gen_alphanum_string_secure(size_t n);

} // namespace random_generators
