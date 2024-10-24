// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/errc.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record.h"
#include "serde/envelope.h"

#include <seastar/core/chunked_fifo.hh>

#include <algorithm>
#include <type_traits>
#include <vector>

namespace cluster {

struct tx_manager_read_request
  : serde::envelope<
      tx_manager_read_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp ntp;
    model::offset start_offset;

    friend bool
    operator==(const tx_manager_read_request&, const tx_manager_read_request&)
      = default;

    auto serde_fields() { return std::tie(ntp, start_offset); }
};

struct tx_manager_read_reply
  : serde::envelope<
      tx_manager_read_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    tx_manager_read_reply() = default;
    explicit tx_manager_read_reply(errc ec)
      : ec(ec) {}

    tx_manager_read_reply(
      fragmented_vector<model::record_batch> batches,
      model::offset log_dirty_offset)
      : ec(errc::success)
      , batches(std::move(batches))
      , log_dirty_offset(log_dirty_offset) {}

    tx_manager_read_reply(tx_manager_read_reply&&) = default;
    tx_manager_read_reply& operator=(tx_manager_read_reply&&) = default;
    /**
     * This type must be copyable to operate with leader_router
     */
    tx_manager_read_reply(const tx_manager_read_reply& o)
      : ec(o.ec)
      , log_dirty_offset(o.log_dirty_offset) {
        for (auto& b : o.batches) {
            batches.push_back(b.copy());
        }
    }

    auto serde_fields() { return std::tie(ec, batches, log_dirty_offset); }

    errc ec;
    fragmented_vector<model::record_batch> batches;
    model::offset log_dirty_offset;

    friend bool operator==(
      const tx_manager_read_reply& lhs, const tx_manager_read_reply& rhs) {
        return lhs.ec == rhs.ec && lhs.batches.size() == rhs.batches.size()
               && std::equal(
                 lhs.batches.begin(), lhs.batches.end(), rhs.batches.begin());
    }
};

struct tx_manager_replicate_request
  : serde::envelope<
      tx_manager_replicate_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    tx_manager_replicate_request() = default;

    tx_manager_replicate_request(
      model::ntp ntp, fragmented_vector<model::record_batch> batches)
      : ntp(std::move(ntp))
      , batches(std::move(batches)) {}

    tx_manager_replicate_request(tx_manager_replicate_request&&) = default;
    tx_manager_replicate_request& operator=(tx_manager_replicate_request&&)
      = default;
    /**
     * This type must be copyable to operate with leader_router
     */
    tx_manager_replicate_request(const tx_manager_replicate_request& o)
      : ntp(o.ntp) {
        for (auto& b : o.batches) {
            batches.push_back(b.copy());
        }
    }

    tx_manager_replicate_request&
    operator=(const tx_manager_replicate_request& other) {
        return *this = tx_manager_replicate_request(other);
    }

    auto serde_fields() { return std::tie(ntp, batches); }

    model::ntp ntp;
    fragmented_vector<model::record_batch> batches;

    friend bool operator==(
      const tx_manager_replicate_request& lhs,
      const tx_manager_replicate_request& rhs) {
        return lhs.ntp == rhs.ntp && lhs.batches.size() == rhs.batches.size()
               && std::equal(
                 lhs.batches.begin(), lhs.batches.end(), rhs.batches.begin());
    }
};

struct tx_manager_replicate_reply
  : serde::envelope<
      tx_manager_replicate_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc ec;

    friend bool operator==(
      const tx_manager_replicate_reply&, const tx_manager_replicate_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

} // namespace cluster
