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

#include <system_error> // bring in std::error_code et al

// use the standard ones instead
#include <boost/outcome/basic_result.hpp>
#include <boost/outcome/std_outcome.hpp>
#include <boost/outcome/std_result.hpp>

// include utils
#include <boost/outcome/iostream_support.hpp>
#include <boost/outcome/try.hpp>
#include <boost/outcome/utils.hpp>

namespace outcome = boost::outcome_v2;

template<class S>
using failure_type = outcome::failure_type<S>;

template<
  class R,
  class S = std::error_code,
  class NoValuePolicy = outcome::policy::default_policy<R, S, void>>
using result = outcome::basic_result<R, S, NoValuePolicy>;

template<class R, class S = std::error_code>
using unchecked = outcome::std_result<R, S, outcome::policy::all_narrow>;

template<class R, class S = std::error_code>
using checked
  = outcome::result<R, S, outcome::policy::throw_bad_result_access<S, void>>;

template<class T>
constexpr bool is_result_v = outcome::is_basic_result_v<T>;
