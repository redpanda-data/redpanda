#pragma once

#include "seastarx.h"

#include <seastar/core/temporary_buffer.hh>

#include <cstdint>
#include <optional>
#include <string_view>

namespace kafka::requests {

using correlation_type = int32_t;

class api_key final {
public:
    constexpr explicit api_key(uint16_t value) noexcept
      : _value(value) {
    }

    constexpr uint16_t value() const {
        return _value;
    }

    constexpr bool operator==(const api_key& other) const {
        return _value == other._value;
    }
    constexpr bool operator!=(const api_key& other) const {
        return _value != other._value;
    }

private:
    uint16_t _value;
};

std::ostream& operator<<(std::ostream&, const api_key&);

class api_version final {
public:
    constexpr explicit api_version(uint16_t value = 0) noexcept
      : _value(value) {
    }

    constexpr uint16_t value() const {
        return _value;
    }

    constexpr bool operator==(const api_version& other) const {
        return _value == other._value;
    }
    constexpr bool operator!=(const api_version& other) const {
        return _value != other._value;
    }
    constexpr bool operator<(const api_version& other) const {
        return _value < other._value;
    }
    constexpr bool operator>(const api_version& other) const {
        return _value > other._value;
    }
    constexpr bool operator<=(const api_version& other) const {
        return _value <= other._value;
    }
    constexpr bool operator>=(const api_version& other) const {
        return _value >= other._value;
    }

private:
    uint16_t _value;
};

std::ostream& operator<<(std::ostream&, const api_version&);

struct request_header {
    api_key key;
    api_version version;
    correlation_type correlation_id;
    temporary_buffer<char> client_id_buffer;
    std::optional<std::string_view> client_id;
};

std::ostream& operator<<(std::ostream&, const request_header&);

struct response_header {
    correlation_type correlation_id;
};

} // namespace kafka::requests
