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

#include "cluster/errc.h"
#include "model/timeout_clock.h"
#include "security/acl.h"
#include "serde/rw/envelope.h"
#include "serde/rw/vector.h"

#include <fmt/format.h>

#include <vector>

namespace cluster {

struct create_acls_cmd_data
  : serde::envelope<
      create_acls_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding> bindings;

    friend bool
    operator==(const create_acls_cmd_data&, const create_acls_cmd_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_acls_cmd_data& r) {
        fmt::print(o, "{{ bindings: {} }}", r.bindings);
        return o;
    }

    auto serde_fields() { return std::tie(bindings); }
};

struct create_acls_request
  : serde::envelope<
      create_acls_request,
      serde::version<0>,
      serde::compat_version<0>> {
    create_acls_cmd_data data;
    model::timeout_clock::duration timeout;

    create_acls_request() noexcept = default;
    create_acls_request(
      create_acls_cmd_data data, model::timeout_clock::duration timeout)
      : data(std::move(data))
      , timeout(timeout) {}

    friend bool
    operator==(const create_acls_request&, const create_acls_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_acls_request& r) {
        fmt::print(o, "{{ data: {}, timeout: {} }}", r.data, r.timeout.count());
        return o;
    }

    auto serde_fields() { return std::tie(data, timeout); }
};

struct create_acls_reply
  : serde::
      envelope<create_acls_reply, serde::version<0>, serde::compat_version<0>> {
    std::vector<errc> results;

    friend bool operator==(const create_acls_reply&, const create_acls_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_acls_reply& r) {
        fmt::print(o, "{{ results: {} }}", r.results);
        return o;
    }

    auto serde_fields() { return std::tie(results); }
};

struct delete_acls_cmd_data
  : serde::envelope<
      delete_acls_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding_filter> filters;

    friend bool
    operator==(const delete_acls_cmd_data&, const delete_acls_cmd_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_cmd_data& d) {
        fmt::print(o, "{{ filters: {} }}", d.filters);
        return o;
    }

    auto serde_fields() { return std::tie(filters); }
};

// result for a single filter
struct delete_acls_result
  : serde::envelope<
      delete_acls_result,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;
    std::vector<security::acl_binding> bindings;

    friend bool operator==(const delete_acls_result&, const delete_acls_result&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_result& r) {
        fmt::print(o, "{{ error: {} bindings: {} }}", r.error, r.bindings);
        return o;
    }

    auto serde_fields() { return std::tie(error, bindings); }
};

struct delete_acls_request
  : serde::envelope<
      delete_acls_request,
      serde::version<0>,
      serde::compat_version<0>> {
    delete_acls_cmd_data data;
    model::timeout_clock::duration timeout;

    delete_acls_request() noexcept = default;
    delete_acls_request(
      delete_acls_cmd_data data, model::timeout_clock::duration timeout)
      : data(std::move(data))
      , timeout(timeout) {}

    friend bool
    operator==(const delete_acls_request&, const delete_acls_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_request& r) {
        fmt::print(o, "{{ data: {} timeout: {} }}", r.data, r.timeout);
        return o;
    }

    auto serde_fields() { return std::tie(data, timeout); }
};

struct delete_acls_reply
  : serde::
      envelope<delete_acls_reply, serde::version<0>, serde::compat_version<0>> {
    std::vector<delete_acls_result> results;

    friend bool operator==(const delete_acls_reply&, const delete_acls_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const delete_acls_reply& r) {
        fmt::print(o, "{{ results: {} }}", r.results);
        return o;
    }

    auto serde_fields() { return std::tie(results); }
};

} // namespace cluster
