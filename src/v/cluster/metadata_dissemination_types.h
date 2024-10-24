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
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "serde/envelope.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/envelope.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/vector.h"
#include "utils/to_string.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/util/bool_class.hh>

#include <fmt/ostream.h>

#include <algorithm>
#include <iosfwd>

namespace cluster {

struct ntp_leader
  : serde::envelope<ntp_leader, serde::version<0>, serde::compat_version<0>> {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;

    ntp_leader() noexcept = default;

    ntp_leader(
      model::ntp ntp,
      model::term_id term,
      std::optional<model::node_id> leader_id)
      : ntp(std::move(ntp))
      , term(term)
      , leader_id(leader_id) {}

    friend bool operator==(const ntp_leader&, const ntp_leader&) = default;

    friend std::ostream& operator<<(std::ostream& o, const ntp_leader& l) {
        fmt::print(
          o,
          "{{ntp: {}, term: {}, leader: {}}}",
          l.ntp,
          l.term,
          l.leader_id ? l.leader_id.value()() : -1);
        return o;
    }

    auto serde_fields() { return std::tie(ntp, term, leader_id); }
};

struct ntp_leader_revision
  : serde::envelope<
      ntp_leader_revision,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision;

    ntp_leader_revision() noexcept = default;

    ntp_leader_revision(
      model::ntp ntp,
      model::term_id term,
      std::optional<model::node_id> leader_id,
      model::revision_id revision)
      : ntp(std::move(ntp))
      , term(term)
      , leader_id(leader_id)
      , revision(revision) {}

    friend bool
    operator==(const ntp_leader_revision&, const ntp_leader_revision&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const ntp_leader_revision& r) {
        fmt::print(
          o,
          "{{ntp: {}, term: {}, leader: {}, revision: {}}}",
          r.ntp,
          r.term,
          r.leader_id,
          r.revision);
        return o;
    }

    auto serde_fields() { return std::tie(ntp, term, leader_id, revision); }
};

struct update_leadership_request_v2
  : serde::envelope<
      update_leadership_request_v2,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t version = 0;
    chunked_vector<ntp_leader_revision> leaders;

    update_leadership_request_v2() noexcept = default;

    friend bool operator==(
      const update_leadership_request_v2& lhs,
      const update_leadership_request_v2& rhs) {
        return std::equal(
          lhs.leaders.begin(),
          lhs.leaders.end(),
          rhs.leaders.begin(),
          rhs.leaders.end());
    };

    update_leadership_request_v2 copy() const {
        chunked_vector<ntp_leader_revision> leaders_cp;
        leaders_cp.reserve(leaders.size());
        std::copy(
          leaders.begin(), leaders.end(), std::back_inserter(leaders_cp));

        return update_leadership_request_v2(std::move(leaders_cp));
    }

    friend std::ostream&
    operator<<(std::ostream& o, const update_leadership_request_v2& r) {
        fmt::print(o, "leaders {}", r.leaders);
        return o;
    }

    explicit update_leadership_request_v2(
      chunked_vector<ntp_leader_revision> leaders)
      : leaders(std::move(leaders)) {}

    auto serde_fields() { return std::tie(leaders); }
};

struct update_leadership_reply
  : serde::envelope<
      update_leadership_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    update_leadership_reply() noexcept = default;

    friend std::ostream&
    operator<<(std::ostream& o, const update_leadership_reply&) {
        fmt::print(o, "{{}}");
        return o;
    }

    auto serde_fields() { return std::tie(); }
};

struct get_leadership_request
  : serde::envelope<
      get_leadership_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    get_leadership_request() noexcept = default;

    friend std::ostream&
    operator<<(std::ostream& o, const get_leadership_request&) {
        fmt::print(o, "{{}}");
        return o;
    }

    auto serde_fields() { return std::tie(); }
};

struct get_leadership_reply
  : serde::envelope<
      get_leadership_reply,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using is_success = ss::bool_class<struct glr_tag>;
    fragmented_vector<ntp_leader> leaders;
    is_success success = is_success::yes;

    get_leadership_reply() noexcept = default;

    explicit get_leadership_reply(
      fragmented_vector<ntp_leader> leaders, is_success success)
      : leaders(std::move(leaders))
      , success(success) {}

    friend bool
    operator==(const get_leadership_reply&, const get_leadership_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const get_leadership_reply& r) {
        fmt::print(o, "leaders {}, success: {}", r.leaders, r.success);
        return o;
    }

    auto serde_fields() { return std::tie(leaders, success); }

    /*
     * Support for serde compat check
     */
    get_leadership_reply copy() const {
        return get_leadership_reply{leaders.copy(), success};
    }
};

} // namespace cluster
