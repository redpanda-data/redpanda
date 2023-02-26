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
#include "bytes/iobuf_parser.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "serde/serde.h"

#include <fmt/ostream.h>

#include <ostream>

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

struct update_leadership_request
  : serde::envelope<
      update_leadership_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<ntp_leader> leaders;

    update_leadership_request() noexcept = default;

    explicit update_leadership_request(std::vector<ntp_leader> leaders)
      : leaders(std::move(leaders)) {}

    friend bool operator==(
      const update_leadership_request&, const update_leadership_request&)
      = default;

    auto serde_fields() { return std::tie(leaders); }

    friend std::ostream&
    operator<<(std::ostream& o, const update_leadership_request& r) {
        fmt::print(o, "leaders {}", r.leaders);
        return o;
    }
};

struct update_leadership_request_v2
  : serde::envelope<
      update_leadership_request_v2,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t version = 0;
    std::vector<ntp_leader_revision> leaders;

    update_leadership_request_v2() noexcept = default;

    friend bool operator==(
      const update_leadership_request_v2&, const update_leadership_request_v2&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const update_leadership_request_v2& r) {
        fmt::print(o, "leaders {}", r.leaders);
        return o;
    }

    explicit update_leadership_request_v2(
      std::vector<ntp_leader_revision> leaders)
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
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<ntp_leader> leaders;

    get_leadership_reply() noexcept = default;

    explicit get_leadership_reply(std::vector<ntp_leader> leaders)
      : leaders(std::move(leaders)) {}

    friend bool
    operator==(const get_leadership_reply&, const get_leadership_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const get_leadership_reply& r) {
        fmt::print(o, "leaders {}", r.leaders);
        return o;
    }

    auto serde_fields() { return std::tie(leaders); }
};

} // namespace cluster
