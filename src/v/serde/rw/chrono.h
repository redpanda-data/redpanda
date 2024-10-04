// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/type_traits.h"
#include "serde/logger.h"
#include "serde/rw/rw.h"

#include <chrono>

namespace serde {

inline constexpr auto max_serializable_ms
  = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::nanoseconds::max());

template<class R, class P>
int64_t checked_duration_cast_to_nanoseconds(
  const std::chrono::duration<R, P>& duration) {
    static_assert(
      __has_builtin(__builtin_mul_overflow),
      "__builtin_mul_overflow not supported.");
    using nano_period = std::chrono::nanoseconds::period;
    using nano_rep = typename std::chrono::nanoseconds::rep;
    // This is how duration_cast determines the output type.
    using output_type = typename std::common_type<R, nano_rep, intmax_t>::type;
    using ratio = std::ratio_divide<P, nano_period>;

    static_assert(
      std::is_same_v<output_type, nano_rep>,
      "Output type is not the same rep as std::chrono::nanoseconds");

    // Extra check to ensure the output type is same as the underlying type
    // supported in lib[std]c++.
    static_assert(
      std::is_same_v<output_type, int64_t>
        || std::is_same_v<output_type, long long>,
      "Output type not in supported integer types.");

    constexpr auto ratio_num = static_cast<output_type>(ratio::num);
    const auto dur = static_cast<output_type>(duration.count());
    output_type mul;
    if (unlikely(__builtin_mul_overflow(dur, ratio_num, &mul))) {
        // Clamp the value.
        // Log here to ensure that it is picked up by duck tape. Ideally this
        // should never happen, but in case it does, we have a log trail.
        //
        // If you are here because your ducktape test failed with this
        // BadLogLine check, it means that you serialized a duration type that
        // caused an overflow when casting to nanoseconds. Clamp your duration
        // to nanoseconds::max().
        using input_type = typename std::chrono::duration<R, P>;
        vlog(
          serde_log.error,
          "Overflow or underflow detected when casting to nanoseconds, "
          "clamping to a limit. Input: {}  type: {}",
          duration.count(),
          type_str<input_type>());
        return duration.count() < 0 ? std::chrono::nanoseconds::min().count()
                                    : std::chrono::nanoseconds::max().count();
    }
    // No overflow/underflow detected, we are safe to cast.
    return std::chrono::duration_cast<std::chrono::nanoseconds>(duration)
      .count();
}

template<typename Rep, typename Period>
void tag_invoke(
  tag_t<write_tag>, iobuf& out, std::chrono::duration<Rep, Period> t) {
    // We explicitly serialize it as ns to avoid any surprises like
    // seastar updating underlying duration types without
    // notice. See https://github.com/redpanda-data/redpanda/pull/5002
    //
    // Check for overflows/underflows.
    // For ex: a millisecond and nanosecond use the same underlying
    // type int64_t but converting from one to other can easily overflow,
    // this is by design.
    // Since we serialize with ns precision, there is a restriction of
    // nanoseconds::max()'s equivalent on the duration to be serialized.
    // On a typical platform which uses int64_t for 'rep', it roughly
    // translates to ~292 years.
    //
    // If we detect an overflow, we will clamp it to maximum supported
    // duration, which is nanosecond::max() and if there is an underflow,
    // we clamp it to minimum supported duration which is nanosecond::min().
    static_assert(
      !std::is_floating_point_v<Rep>,
      "Floating point duration conversions are prone to precision and "
      "rounding issues.");
    write<int64_t>(out, checked_duration_cast_to_nanoseconds(t));
}

template<class R, class P>
void tag_invoke(
  tag_t<read_tag>,
  iobuf_parser& in,
  std::chrono::duration<R, P>& t,
  const std::size_t bytes_left_limit) {
    using Type = std::chrono::duration<R, P>;

    static_assert(
      !std::is_floating_point_v<R>,
      "Floating point duration conversions are prone to precision and "
      "rounding issues.");
    auto rep = read_nested<int64_t>(in, bytes_left_limit);
    t = std::chrono::duration_cast<Type>(std::chrono::nanoseconds{rep});
}

template<typename Clock, typename Duration>
void write(iobuf&, std::chrono::time_point<Clock, Duration> t) {
    static_assert(
      base::unsupported_type<decltype(t)>::value,
      "Time point serialization is risky and can have unintended "
      "consequences. Check with Redpanda team before fixing this.");
}

template<typename Clock, typename Duration>
void read(
  iobuf_parser&,
  std::chrono::time_point<Clock, Duration>& t,
  const std::size_t /* bytes_left_limit */) {
    static_assert(
      base::unsupported_type<decltype(t)>::value,
      "Time point serialization is risky and can have unintended "
      "consequences. Check with Redpanda team before fixing this.");
}

} // namespace serde
