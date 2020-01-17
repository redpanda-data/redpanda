#pragma once

#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <cstdint>
#include <iosfwd>
#include <limits>

namespace model {

enum class timestamp_type : uint8_t { create_time, append_time };

std::ostream& operator<<(std::ostream&, timestamp_type);

class timestamp {
public:
    using type = int64_t;

    timestamp() noexcept = default;

    constexpr explicit timestamp(type v) noexcept
      : _v(v) {}

    constexpr type value() const noexcept { return _v; }

    constexpr static timestamp min() noexcept { return timestamp(0); }

    constexpr static timestamp max() noexcept {
        return timestamp(std::numeric_limits<type>::max());
    }

    constexpr static timestamp missing() noexcept { return timestamp(-1); }

    bool operator<(const timestamp& other) const { return _v < other._v; }

    bool operator<=(const timestamp& other) const { return _v <= other._v; }

    bool operator>(const timestamp& other) const { return other._v < _v; }

    bool operator>=(const timestamp& other) const { return other._v <= _v; }

    bool operator==(const timestamp& other) const { return _v == other._v; }

    bool operator!=(const timestamp& other) const { return !(*this == other); }

    friend std::ostream& operator<<(std::ostream&, timestamp);

private:
    type _v = missing().value();
};

using timestamp_clock = std::chrono::system_clock;

static inline timestamp new_timestamp() {
    return timestamp(timestamp_clock::now().time_since_epoch().count());
}

} // namespace model
