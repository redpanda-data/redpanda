/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "reflection/adl.h"
#include "seastarx.h"
#include "serde/envelope.h"
#include "vlog.h"

#include <seastar/core/sstring.hh>

#include <exception>
#include <string_view>

namespace v8_engine {

class data_policy_exeption final : public std::exception {
public:
    explicit data_policy_exeption(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

// Datapolicy property for v8_engine. In first version it contains only
// function_name and script_name, in the future it will contain ACLs, geo,
// e.t.c.
class data_policy : public serde::envelope<data_policy, serde::version<0>> {
public:
    static constexpr int8_t version{1};

    data_policy() = default;
    data_policy(ss::sstring fn, ss::sstring sn) noexcept
      : _function_name(std::move(fn))
      , _script_name(std::move(sn)) {}

    const ss::sstring& function_name() const { return _function_name; }
    const ss::sstring& script_name() const { return _script_name; }

    friend bool operator==(const data_policy&, const data_policy&) = default;
    auto serde_fields() { return std::tie(_function_name, _script_name); }

private:
    ss::sstring _function_name;
    ss::sstring _script_name;

    friend std::ostream&
    operator<<(std::ostream& os, const data_policy& datapolicy);
};

} // namespace v8_engine

namespace reflection {

template<>
struct adl<v8_engine::data_policy> {
    void to(iobuf& out, v8_engine::data_policy&& dp);
    v8_engine::data_policy from(iobuf_parser& in);
};

} // namespace reflection
