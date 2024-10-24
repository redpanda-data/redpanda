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

#include "base/seastarx.h"
#include "config/property.h"
#include "security/acl.h"
#include "security/mtls_rule.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/core.h>

#include <optional>
#include <regex>
#include <string_view>

namespace security::tls {

class principal_mapper {
public:
    explicit principal_mapper(
      config::binding<std::optional<std::vector<ss::sstring>>> cb);
    std::optional<ss::sstring> apply(std::string_view sv) const;

private:
    friend struct fmt::formatter<principal_mapper>;

    friend std::ostream&
    operator<<(std::ostream& os, const principal_mapper& p);

    config::binding<std::optional<std::vector<ss::sstring>>> _binding;
    std::vector<rule> _rules;
};

class mtls_state {
public:
    explicit mtls_state(
      ss::sstring principal, std::optional<ss::sstring> subject)
      : _principal{principal_type::user, std::move(principal)}
      , _subject(std::move(subject)) {}

    const acl_principal& principal() const { return _principal; }

    const std::optional<ss::sstring>& subject() const { return _subject; }

private:
    acl_principal _principal;
    std::optional<ss::sstring> _subject;
};

} // namespace security::tls

template<>
struct fmt::formatter<security::tls::principal_mapper> {
    using type = security::tls::principal_mapper;

    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    typename FormatContext::iterator
    format(const type& r, FormatContext& ctx) const;
};
