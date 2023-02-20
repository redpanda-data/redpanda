/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "seastarx.h"
#include "serde/serde.h"

#include <seastar/core/sstring.hh>

#include <boost/date_time/gregorian/gregorian_types.hpp>
#include <fmt/core.h>

#include <exception>
#include <fstream>
#include <vector>

namespace security {

class license_exception : public std::exception {
public:
    explicit license_exception(ss::sstring s) noexcept
      : _msg(std::move(s)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

class license_invalid_exception final : public license_exception {
public:
    explicit license_invalid_exception(ss::sstring s) noexcept
      : license_exception(std::move(s)) {}
};

class license_malformed_exception final : public license_exception {
public:
    explicit license_malformed_exception(ss::sstring s) noexcept
      : license_exception(std::move(s)) {}
};

class license_verifcation_exception final : public license_exception {
public:
    explicit license_verifcation_exception(ss::sstring s) noexcept
      : license_exception(std::move(s)) {}
};

enum class license_type : uint8_t { free_trial = 0, enterprise = 1 };

ss::sstring license_type_to_string(license_type type);

inline std::ostream& operator<<(std::ostream& os, license_type lt) {
    os << license_type_to_string(lt);
    return os;
}

struct license
  : serde::envelope<license, serde::version<1>, serde::compat_version<0>> {
    /// Expected encoded contents
    uint8_t format_version;
    license_type type;
    ss::sstring organization;
    std::chrono::seconds expiry;
    ss::sstring checksum;

    auto serde_fields() {
        return std::tie(format_version, type, organization, expiry, checksum);
    }

    /// true if todays date is greater then \ref expiry
    bool is_expired() const noexcept;

    /// Seconds since epoch until license expiration
    std::chrono::seconds expires() const noexcept;

    auto operator<=>(const license&) const = delete;

private:
    friend struct fmt::formatter<license>;

    friend bool operator==(const license& a, const license& b) = default;

    friend std::ostream& operator<<(std::ostream& os, const license& lic);
};

/// Returns a license or an exception indicating the reason why the method
/// failed, reasons could be:
/// 1. Malformed license
/// 2. Invalid license
license make_license(const ss::sstring& raw_license);

} // namespace security

namespace fmt {
template<>
struct formatter<security::license> {
    using type = security::license;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};

} // namespace fmt
