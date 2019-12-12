#pragma once

#include <system_error> // bring in std::error_code et al

// use the standard ones instead
#include <boost/outcome/std_outcome.hpp>
#include <boost/outcome/std_result.hpp>

// include utils
#include <boost/outcome/iostream_support.hpp>
#include <boost/outcome/try.hpp>
#include <boost/outcome/utils.hpp>

namespace outcome = boost::outcome_v2;

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
