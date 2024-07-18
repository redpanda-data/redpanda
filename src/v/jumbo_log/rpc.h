// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "cluster/errc.h"
#include "jumbo_log/metadata.h"
#include "model/timeout_clock.h"
#include "serde/envelope.h"

#include <seastar/core/lowres_clock.hh>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <ostream>
#include <tuple>
#include <vector>

namespace jumbo_log::rpc {

struct create_write_intent_request
  : serde::envelope<
      create_write_intent_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::timeout_clock::duration timeout;
    jumbo_log::segment_object object;

    create_write_intent_request() noexcept = default;

    explicit create_write_intent_request(
      model::timeout_clock::duration timeout, jumbo_log::segment_object object)
      : timeout(timeout)
      , object(object) {}

    friend bool operator==(
      const create_write_intent_request&, const create_write_intent_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_write_intent_request& req) {
        fmt::print(
          o, "timeout: {}, object: {}", req.timeout.count(), req.object);
        return o;
    }

    auto serde_fields() { return std::tie(object, timeout); }
};

struct create_write_intent_reply
  : serde::envelope<
      create_write_intent_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    jumbo_log::write_intent_id_t write_intent_id;
    cluster::errc ec;

    create_write_intent_reply() noexcept = default;

    create_write_intent_reply(
      jumbo_log::write_intent_id_t write_intent_id, cluster::errc ec)
      : write_intent_id(write_intent_id)
      , ec(ec) {}

    friend bool operator==(
      const create_write_intent_reply&, const create_write_intent_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const create_write_intent_reply& rep) {
        fmt::print(
          o, "write_intent_id: {}, ec: {}", rep.write_intent_id, rep.ec);
        return o;
    }

    auto serde_fields() { return std::tie(write_intent_id, ec); }
};

struct get_write_intents_request
  : serde::envelope<
      get_write_intents_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::timeout_clock::duration timeout;
    std::vector<write_intent_id_t> write_intent_ids;

    get_write_intents_request() noexcept = default;

    explicit get_write_intents_request(
      model::timeout_clock::duration timeout,
      std::vector<write_intent_id_t> ids)
      : timeout(timeout)
      , write_intent_ids(std::move(ids)) {}

    friend bool operator==(
      const get_write_intents_request&, const get_write_intents_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const get_write_intents_request& req) {
        fmt::print(
          o,
          "timeout: {}, write_intent_ids: {}",
          req.timeout.count(),
          fmt::join(req.write_intent_ids, ", "));
        return o;
    }

    auto serde_fields() { return std::tie(write_intent_ids, timeout); }
};

struct get_write_intents_reply
  : serde::envelope<
      get_write_intents_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<write_intent_segment> write_intents;
    cluster::errc ec;

    get_write_intents_reply() noexcept = default;

    get_write_intents_reply(
      std::vector<write_intent_segment> segments, cluster::errc ec)
      : write_intents(std::move(segments))
      , ec(ec) {}

    friend bool
    operator==(const get_write_intents_reply&, const get_write_intents_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const get_write_intents_reply& rep) {
        fmt::print(
          o,
          "write_intents: {}, ec: {}",
          fmt::join(rep.write_intents, ", "),
          rep.ec);
        return o;
    }

    auto serde_fields() { return std::tie(write_intents, ec); }
};
} // namespace jumbo_log::rpc
