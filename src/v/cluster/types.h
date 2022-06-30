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
#include "cluster/fwd.h"
#include "kafka/types.h"
#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/types.h"
#include "security/acl.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "storage/ntp_config.h"
#include "tristate.h"
#include "utils/to_string.h"
#include "v8_engine/data_policy.h"

#include <seastar/core/sstring.hh>

#include <absl/container/btree_set.h>
#include <fmt/format.h>

#include <cstdint>
#include <optional>

namespace cluster {
using consensus_ptr = ss::lw_shared_ptr<raft::consensus>;
using broker_ptr = ss::lw_shared_ptr<model::broker>;

// A cluster version is a logical protocol version describing the content
// of the raft0 on disk structures, and available features.  These are
// passed over the network via the health_manager, and persisted in
// the feature_manager
using cluster_version = named_type<int64_t, struct cluster_version_tag>;
constexpr cluster_version invalid_version = cluster_version{-1};

struct allocate_id_request
  : serde::envelope<allocate_id_request, serde::version<0>> {
    model::timeout_clock::duration timeout;

    allocate_id_request() noexcept = default;

    explicit allocate_id_request(model::timeout_clock::duration timeout)
      : timeout(timeout) {}

    friend bool
    operator==(const allocate_id_request&, const allocate_id_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct allocate_id_reply
  : serde::envelope<allocate_id_reply, serde::version<0>> {
    int64_t id;
    errc ec;

    allocate_id_reply() noexcept = default;

    allocate_id_reply(int64_t id, errc ec)
      : id(id)
      , ec(ec) {}

    friend bool operator==(const allocate_id_reply&, const allocate_id_reply&)
      = default;

    auto serde_fields() { return std::tie(id, ec); }
};

enum class tx_errc {
    none = 0,
    leader_not_found,
    shard_not_found,
    partition_not_found,
    stm_not_found,
    partition_not_exists,
    pid_not_found,
    // when a request times out a client should not do any assumptions about its
    // effect. the request may time out before reaching the server, the request
    // may be successuly processed or may fail and the reply times out
    timeout,
    conflict,
    fenced,
    stale,
    not_coordinator,
    coordinator_not_available,
    preparing_rebalance,
    rebalance_in_progress,
    coordinator_load_in_progress,
    // an unspecified error happened, the effect of the request is unknown
    // the error code is very similar to timeout
    unknown_server_error,
    // an unspecified error happened, a client may assume it had zero effect on
    // the target node
    request_rejected,
    invalid_producer_id_mapping,
    invalid_txn_state
};
struct tx_errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::tx_errc"; }

    std::string message(int c) const final {
        switch (static_cast<tx_errc>(c)) {
        case tx_errc::none:
            return "None";
        case tx_errc::leader_not_found:
            return "Leader not found";
        case tx_errc::shard_not_found:
            return "Shard not found";
        case tx_errc::partition_not_found:
            return "Partition not found";
        case tx_errc::stm_not_found:
            return "Stm not found";
        case tx_errc::partition_not_exists:
            return "Partition not exists";
        case tx_errc::pid_not_found:
            return "Pid not found";
        case tx_errc::timeout:
            return "Timeout";
        case tx_errc::conflict:
            return "Conflict";
        case tx_errc::fenced:
            return "Fenced";
        case tx_errc::stale:
            return "Stale";
        case tx_errc::not_coordinator:
            return "Not coordinator";
        case tx_errc::coordinator_not_available:
            return "Coordinator not available";
        case tx_errc::preparing_rebalance:
            return "Preparing rebalance";
        case tx_errc::rebalance_in_progress:
            return "Rebalance in progress";
        case tx_errc::coordinator_load_in_progress:
            return "Coordinator load in progress";
        case tx_errc::unknown_server_error:
            return "Unknown server error";
        case tx_errc::request_rejected:
            return "Request rejected";
        default:
            return "cluster::tx_errc::unknown";
        }
    }
};
inline const std::error_category& tx_error_category() noexcept {
    static tx_errc_category e;
    return e;
}
inline std::error_code make_error_code(tx_errc e) noexcept {
    return std::error_code(static_cast<int>(e), tx_error_category());
}

struct try_abort_request
  : serde::envelope<try_abort_request, serde::version<0>> {
    model::partition_id tm;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;

    try_abort_request() noexcept = default;

    try_abort_request(
      model::partition_id tm,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : tm(tm)
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout) {}

    friend bool operator==(const try_abort_request&, const try_abort_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        tm = read_nested<model::partition_id>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, tm);
        write(out, pid);
        write(out, tx_seq);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct try_abort_reply : serde::envelope<try_abort_reply, serde::version<0>> {
    using committed_type = ss::bool_class<struct committed_type_tag>;
    using aborted_type = ss::bool_class<struct aborted_type_tag>;

    committed_type commited;
    aborted_type aborted;
    tx_errc ec;

    try_abort_reply() noexcept = default;

    try_abort_reply(committed_type committed, aborted_type aborted, tx_errc ec)
      : commited(committed)
      , aborted(aborted)
      , ec(ec) {}

    explicit try_abort_reply(tx_errc ec)
      : ec(ec) {}

    friend bool operator==(const try_abort_reply&, const try_abort_reply&)
      = default;

    static try_abort_reply make_aborted() {
        return {committed_type::no, aborted_type::yes, tx_errc::none};
    }

    static try_abort_reply make_committed() {
        return {committed_type::yes, aborted_type::no, tx_errc::none};
    }

    auto serde_fields() { return std::tie(commited, aborted, ec); }
};

struct init_tm_tx_request
  : serde::envelope<init_tm_tx_request, serde::version<0>> {
    kafka::transactional_id tx_id;
    std::chrono::milliseconds transaction_timeout_ms;
    model::timeout_clock::duration timeout;

    init_tm_tx_request() noexcept = default;

    init_tm_tx_request(
      kafka::transactional_id tx_id,
      std::chrono::milliseconds tx_timeout,
      model::timeout_clock::duration timeout)
      : tx_id(std::move(tx_id))
      , transaction_timeout_ms(tx_timeout)
      , timeout(timeout) {}

    friend bool operator==(const init_tm_tx_request&, const init_tm_tx_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        tx_id = read_nested<kafka::transactional_id>(in, h._bytes_left_limit);
        transaction_timeout_ms = read_nested<std::chrono::milliseconds>(
          in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, tx_id);
        write(out, transaction_timeout_ms);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct init_tm_tx_reply : serde::envelope<init_tm_tx_reply, serde::version<0>> {
    // partition_not_exists, not_leader, topic_not_exists
    model::producer_identity pid;
    tx_errc ec;

    init_tm_tx_reply() noexcept = default;

    init_tm_tx_reply(model::producer_identity pid, tx_errc ec)
      : pid(pid)
      , ec(ec) {}

    friend bool operator==(const init_tm_tx_reply&, const init_tm_tx_reply&)
      = default;

    explicit init_tm_tx_reply(tx_errc ec)
      : ec(ec) {}

    auto serde_fields() { return std::tie(pid, ec); }
};

struct add_paritions_tx_request {
    struct topic {
        model::topic name{};
        std::vector<model::partition_id> partitions{};
    };
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    std::vector<topic> topics{};
};
struct add_paritions_tx_reply {
    struct partition_result {
        model::partition_id partition_index{};
        tx_errc error_code{};
    };
    struct topic_result {
        model::topic name{};
        std::vector<add_paritions_tx_reply::partition_result> results{};
    };
    std::vector<add_paritions_tx_reply::topic_result> results{};
};
struct add_offsets_tx_request {
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    kafka::group_id group_id{};
};
struct add_offsets_tx_reply {
    tx_errc error_code{};
};
struct end_tx_request {
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    bool committed{};
};
struct end_tx_reply {
    tx_errc error_code{};
};
struct begin_tx_request : serde::envelope<begin_tx_request, serde::version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    std::chrono::milliseconds transaction_timeout_ms;

    begin_tx_request() noexcept = default;

    begin_tx_request(
      model::ntp ntp,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      std::chrono::milliseconds transaction_timeout_ms)
      : ntp(std::move(ntp))
      , pid(pid)
      , tx_seq(tx_seq)
      , transaction_timeout_ms(transaction_timeout_ms) {}

    friend bool operator==(const begin_tx_request&, const begin_tx_request&)
      = default;

    auto serde_fields() {
        return std::tie(ntp, pid, tx_seq, transaction_timeout_ms);
    }
};

struct begin_tx_reply : serde::envelope<begin_tx_reply, serde::version<0>> {
    model::ntp ntp;
    model::term_id etag;
    tx_errc ec;

    begin_tx_reply() noexcept = default;

    begin_tx_reply(model::ntp ntp, model::term_id etag, tx_errc ec)
      : ntp(std::move(ntp))
      , etag(etag)
      , ec(ec) {}

    begin_tx_reply(model::ntp ntp, tx_errc ec)
      : ntp(std::move(ntp))
      , ec(ec) {}

    friend bool operator==(const begin_tx_reply&, const begin_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ntp, etag, ec); }
};

struct prepare_tx_request
  : serde::envelope<prepare_tx_request, serde::version<0>> {
    model::ntp ntp;
    model::term_id etag;
    model::partition_id tm;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;

    prepare_tx_request() noexcept = default;

    prepare_tx_request(
      model::ntp ntp,
      model::term_id etag,
      model::partition_id tm,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : ntp(std::move(ntp))
      , etag(etag)
      , tm(tm)
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout) {}

    friend bool operator==(const prepare_tx_request&, const prepare_tx_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        ntp = read_nested<model::ntp>(in, h._bytes_left_limit);
        etag = read_nested<model::term_id>(in, h._bytes_left_limit);
        tm = read_nested<model::partition_id>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, ntp);
        write(out, etag);
        write(out, tm);
        write(out, pid);
        write(out, tx_seq);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct prepare_tx_reply : serde::envelope<prepare_tx_reply, serde::version<0>> {
    tx_errc ec;

    prepare_tx_reply() noexcept = default;

    explicit prepare_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool operator==(const prepare_tx_reply&, const prepare_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct commit_tx_request
  : serde::envelope<commit_tx_request, serde::version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;

    commit_tx_request() noexcept = default;

    commit_tx_request(
      model::ntp ntp,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : ntp(std::move(ntp))
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout) {}

    friend bool operator==(const commit_tx_request&, const commit_tx_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        ntp = read_nested<model::ntp>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, ntp);
        write(out, pid);
        write(out, tx_seq);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct commit_tx_reply : serde::envelope<commit_tx_reply, serde::version<0>> {
    tx_errc ec;

    commit_tx_reply() noexcept = default;

    explicit commit_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool operator==(const commit_tx_reply&, const commit_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct abort_tx_request : serde::envelope<abort_tx_request, serde::version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;

    abort_tx_request() noexcept = default;

    abort_tx_request(
      model::ntp ntp,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : ntp(std::move(ntp))
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout) {}

    friend bool operator==(const abort_tx_request&, const abort_tx_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        ntp = read_nested<model::ntp>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, ntp);
        write(out, pid);
        write(out, tx_seq);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct abort_tx_reply : serde::envelope<abort_tx_reply, serde::version<0>> {
    tx_errc ec;

    abort_tx_reply() noexcept = default;

    explicit abort_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool operator==(const abort_tx_reply&, const abort_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct begin_group_tx_request
  : serde::envelope<begin_group_tx_request, serde::version<0>> {
    model::ntp ntp;
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;

    begin_group_tx_request() noexcept = default;

    begin_group_tx_request(
      model::ntp ntp,
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : ntp(std::move(ntp))
      , group_id(std::move(group_id))
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout) {}

    /*
     * construct with default value model::ntp
     * https://github.com/redpanda-data/redpanda/issues/5055
     */
    begin_group_tx_request(
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : begin_group_tx_request(
        model::ntp(), std::move(group_id), pid, tx_seq, timeout) {}

    friend bool
    operator==(const begin_group_tx_request&, const begin_group_tx_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        ntp = read_nested<model::ntp>(in, h._bytes_left_limit);
        group_id = read_nested<kafka::group_id>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, ntp);
        write(out, group_id);
        write(out, pid);
        write(out, tx_seq);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct begin_group_tx_reply
  : serde::envelope<begin_group_tx_reply, serde::version<0>> {
    model::term_id etag;
    tx_errc ec;

    begin_group_tx_reply() noexcept = default;

    explicit begin_group_tx_reply(tx_errc ec)
      : ec(ec) {}

    begin_group_tx_reply(model::term_id etag, tx_errc ec)
      : etag(etag)
      , ec(ec) {}

    friend bool
    operator==(const begin_group_tx_reply&, const begin_group_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(etag, ec); }
};

struct prepare_group_tx_request
  : serde::envelope<prepare_group_tx_request, serde::version<0>> {
    model::ntp ntp;
    kafka::group_id group_id;
    model::term_id etag;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;

    prepare_group_tx_request() noexcept = default;

    prepare_group_tx_request(
      model::ntp ntp,
      kafka::group_id group_id,
      model::term_id etag,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : ntp(std::move(ntp))
      , group_id(std::move(group_id))
      , etag(etag)
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout) {}

    /*
     * construct with default value model::ntp
     * https://github.com/redpanda-data/redpanda/issues/5055
     */
    prepare_group_tx_request(
      kafka::group_id group_id,
      model::term_id etag,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : prepare_group_tx_request(
        model::ntp(), std::move(group_id), etag, pid, tx_seq, timeout) {}

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        ntp = read_nested<model::ntp>(in, h._bytes_left_limit);
        group_id = read_nested<kafka::group_id>(in, h._bytes_left_limit);
        etag = read_nested<model::term_id>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, ntp);
        write(out, group_id);
        write(out, etag);
        write(out, pid);
        write(out, tx_seq);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct prepare_group_tx_reply
  : serde::envelope<prepare_group_tx_reply, serde::version<0>> {
    tx_errc ec;

    prepare_group_tx_reply() noexcept = default;

    explicit prepare_group_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool
    operator==(const prepare_group_tx_reply&, const prepare_group_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct commit_group_tx_request
  : serde::envelope<commit_group_tx_request, serde::version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    kafka::group_id group_id;
    model::timeout_clock::duration timeout;

    commit_group_tx_request() noexcept = default;

    commit_group_tx_request(
      model::ntp ntp,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      kafka::group_id group_id,
      model::timeout_clock::duration timeout)
      : ntp(std::move(ntp))
      , pid(pid)
      , tx_seq(tx_seq)
      , group_id(std::move(group_id))
      , timeout(timeout) {}

    /*
     * construct with default value model::ntp
     * https://github.com/redpanda-data/redpanda/issues/5055
     */
    commit_group_tx_request(
      model::producer_identity pid,
      model::tx_seq tx_seq,
      kafka::group_id group_id,
      model::timeout_clock::duration timeout)
      : commit_group_tx_request(
        model::ntp(), pid, tx_seq, std::move(group_id), timeout) {}

    friend bool
    operator==(const commit_group_tx_request&, const commit_group_tx_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        ntp = read_nested<model::ntp>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        group_id = read_nested<kafka::group_id>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, ntp);
        write(out, pid);
        write(out, tx_seq);
        write(out, group_id);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct commit_group_tx_reply
  : serde::envelope<commit_group_tx_reply, serde::version<0>> {
    tx_errc ec;

    commit_group_tx_reply() noexcept = default;

    explicit commit_group_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool
    operator==(const commit_group_tx_reply&, const commit_group_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

struct abort_group_tx_request
  : serde::envelope<abort_group_tx_request, serde::version<0>> {
    model::ntp ntp;
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout;

    abort_group_tx_request() noexcept = default;

    abort_group_tx_request(
      model::ntp ntp,
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : ntp(std::move(ntp))
      , group_id(std::move(group_id))
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout) {}

    /*
     * construct with default value model::ntp
     * https://github.com/redpanda-data/redpanda/issues/5055
     */
    abort_group_tx_request(
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout)
      : abort_group_tx_request(
        model::ntp(), std::move(group_id), pid, tx_seq, timeout) {}

    friend bool
    operator==(const abort_group_tx_request&, const abort_group_tx_request&)
      = default;

    auto serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        ntp = read_nested<model::ntp>(in, h._bytes_left_limit);
        group_id = read_nested<kafka::group_id>(in, h._bytes_left_limit);
        pid = read_nested<model::producer_identity>(in, h._bytes_left_limit);
        tx_seq = read_nested<model::tx_seq>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    auto serde_write(iobuf& out) {
        using serde::write;
        write(out, ntp);
        write(out, group_id);
        write(out, pid);
        write(out, tx_seq);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct abort_group_tx_reply
  : serde::envelope<abort_group_tx_reply, serde::version<0>> {
    tx_errc ec;

    abort_group_tx_reply() noexcept = default;

    explicit abort_group_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool
    operator==(const abort_group_tx_reply&, const abort_group_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }
};

/// Old-style request sent by node to join raft-0
/// - Does not specify logical version
/// - Always specifies node_id
/// (remove this RPC two versions after join_node_request was
///  added to replace it)
struct join_request : serde::envelope<join_request, serde::version<0>> {
    join_request() noexcept = default;

    explicit join_request(model::broker b)
      : node(std::move(b)) {}

    model::broker node;

    friend bool operator==(const join_request&, const join_request&) = default;

    auto serde_fields() { return std::tie(node); }
};

struct join_reply : serde::envelope<join_reply, serde::version<0>> {
    bool success;

    join_reply() noexcept = default;

    explicit join_reply(bool success)
      : success(success) {}

    friend bool operator==(const join_reply&, const join_reply&) = default;

    auto serde_fields() { return std::tie(success); }
};

/// Successor to join_request:
/// - Include version metadata for joining node
/// - Has fields for implementing auto-selection of
///   node_id (https://github.com/redpanda-data/redpanda/issues/2793)
///   in future.
struct join_node_request
  : serde::envelope<join_node_request, serde::version<0>> {
    join_node_request() noexcept = default;

    explicit join_node_request(
      cluster_version lv, std::vector<uint8_t> nuuid, model::broker b)
      : logical_version(lv)
      , node_uuid(nuuid)
      , node(std::move(b)) {}

    explicit join_node_request(cluster_version lv, model::broker b)
      : logical_version(lv)
      , node(std::move(b)) {}

    static constexpr int8_t current_version = 1;
    cluster_version logical_version;

    // node_uuid may be empty: this is for future use implementing auto
    // selection of node_id.  Convert to a more convenient type later:
    // the vector is just to reserve the on-disk layout.
    std::vector<uint8_t> node_uuid;
    model::broker node;

    friend bool operator==(const join_node_request&, const join_node_request&)
      = default;

    auto serde_fields() { return std::tie(logical_version, node_uuid, node); }
};

struct join_node_reply : serde::envelope<join_node_reply, serde::version<0>> {
    bool success{false};
    model::node_id id{-1};

    join_node_reply() noexcept = default;

    join_node_reply(bool success, model::node_id id)
      : success(success)
      , id(id) {}

    friend bool operator==(const join_node_reply&, const join_node_reply&)
      = default;

    auto serde_fields() { return std::tie(success, id); }
};

struct configuration_update_request
  : serde::envelope<configuration_update_request, serde::version<0>> {
    configuration_update_request() noexcept = default;
    explicit configuration_update_request(model::broker b, model::node_id tid)
      : node(std::move(b))
      , target_node(tid) {}

    model::broker node;
    model::node_id target_node;

    friend bool operator==(
      const configuration_update_request&, const configuration_update_request&)
      = default;

    auto serde_fields() { return std::tie(node, target_node); }
};

struct configuration_update_reply
  : serde::envelope<configuration_update_reply, serde::version<0>> {
    configuration_update_reply() noexcept = default;
    explicit configuration_update_reply(bool success)
      : success(success) {}

    bool success;

    friend bool operator==(
      const configuration_update_reply&, const configuration_update_reply&)
      = default;

    auto serde_fields() { return std::tie(success); }
};

/// Partition assignment describes an assignment of all replicas for single NTP.
/// The replicas are hold in vector of broker_shard.
struct partition_assignment
  : serde::envelope<partition_assignment, serde::version<0>> {
    partition_assignment() noexcept = default;
    partition_assignment(
      raft::group_id group,
      model::partition_id id,
      std::vector<model::broker_shard> replicas)
      : group(group)
      , id(id)
      , replicas(std::move(replicas)) {}

    raft::group_id group;
    model::partition_id id;
    std::vector<model::broker_shard> replicas;

    model::partition_metadata create_partition_metadata() const {
        auto p_md = model::partition_metadata(id);
        p_md.replicas = replicas;
        return p_md;
    }

    auto serde_fields() { return std::tie(group, id, replicas); }
    friend std::ostream& operator<<(std::ostream&, const partition_assignment&);

    friend bool
    operator==(const partition_assignment&, const partition_assignment&)
      = default;
};

struct remote_topic_properties
  : serde::envelope<remote_topic_properties, serde::version<0>> {
    remote_topic_properties() = default;
    remote_topic_properties(
      model::initial_revision_id remote_revision,
      int32_t remote_partition_count)
      : remote_revision(remote_revision)
      , remote_partition_count(remote_partition_count) {}

    model::initial_revision_id remote_revision;
    int32_t remote_partition_count;

    friend std::ostream&
    operator<<(std::ostream&, const remote_topic_properties&);

    auto serde_fields() {
        return std::tie(remote_revision, remote_partition_count);
    }

    friend bool
    operator==(const remote_topic_properties&, const remote_topic_properties&)
      = default;
};

/**
 * Structure holding topic properties overrides, empty values will be replaced
 * with defaults
 */
struct topic_properties
  : serde::
      envelope<topic_properties, serde::version<1>, serde::compat_version<0>> {
    topic_properties() noexcept = default;
    topic_properties(
      std::optional<model::compression> compression,
      std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags,
      std::optional<model::compaction_strategy> compaction_strategy,
      std::optional<model::timestamp_type> timestamp_type,
      std::optional<size_t> segment_size,
      tristate<size_t> retention_bytes,
      tristate<std::chrono::milliseconds> retention_duration,
      std::optional<bool> recovery,
      std::optional<model::shadow_indexing_mode> shadow_indexing,
      std::optional<bool> read_replica,
      std::optional<ss::sstring> read_replica_bucket,
      std::optional<remote_topic_properties> remote_topic_properties)
      : compression(compression)
      , cleanup_policy_bitflags(cleanup_policy_bitflags)
      , compaction_strategy(compaction_strategy)
      , timestamp_type(timestamp_type)
      , segment_size(segment_size)
      , retention_bytes(retention_bytes)
      , retention_duration(retention_duration)
      , recovery(recovery)
      , shadow_indexing(shadow_indexing)
      , read_replica(read_replica)
      , read_replica_bucket(read_replica_bucket)
      , remote_topic_properties(remote_topic_properties) {}

    std::optional<model::compression> compression;
    std::optional<model::cleanup_policy_bitflags> cleanup_policy_bitflags;
    std::optional<model::compaction_strategy> compaction_strategy;
    std::optional<model::timestamp_type> timestamp_type;
    std::optional<size_t> segment_size;
    tristate<size_t> retention_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_duration{std::nullopt};
    std::optional<bool> recovery;
    std::optional<model::shadow_indexing_mode> shadow_indexing;
    std::optional<bool> read_replica;
    std::optional<ss::sstring> read_replica_bucket;
    std::optional<remote_topic_properties> remote_topic_properties;

    bool is_compacted() const;
    bool has_overrides() const;

    storage::ntp_config::default_overrides get_ntp_cfg_overrides() const;

    friend std::ostream& operator<<(std::ostream&, const topic_properties&);
    auto serde_fields() {
        return std::tie(
          compression,
          cleanup_policy_bitflags,
          compaction_strategy,
          timestamp_type,
          segment_size,
          retention_bytes,
          retention_duration,
          recovery,
          shadow_indexing,
          read_replica,
          read_replica_bucket,
          remote_topic_properties);
    }

    friend bool operator==(const topic_properties&, const topic_properties&)
      = default;
};

enum incremental_update_operation : int8_t { none, set, remove };
template<typename T>

struct property_update
  : serde::envelope<property_update<T>, serde::version<0>> {
    property_update() = default;
    property_update(T v, incremental_update_operation op)
      : value(std::move(v))
      , op(op) {}

    T value;
    incremental_update_operation op = incremental_update_operation::none;

    auto serde_fields() { return std::tie(value, op); }

    friend bool operator==(const property_update<T>&, const property_update<T>&)
      = default;
};

template<typename T>
struct property_update<tristate<T>>
  : serde::envelope<property_update<tristate<T>>, serde::version<0>> {
    property_update()
      : value(std::nullopt){};

    property_update(tristate<T> v, incremental_update_operation op)
      : value(std::move(v))
      , op(op) {}
    tristate<T> value;
    incremental_update_operation op = incremental_update_operation::none;

    auto serde_fields() { return std::tie(value, op); }

    friend bool operator==(
      const property_update<tristate<T>>&, const property_update<tristate<T>>&)
      = default;
};

struct incremental_topic_updates
  : serde::envelope<incremental_topic_updates, serde::version<0>> {
    static constexpr int8_t version_with_data_policy = -1;
    static constexpr int8_t version_with_shadow_indexing = -3;
    // negative version indicating different format:
    // -1 - topic_updates with data_policy
    // -2 - topic_updates without data_policy
    // -3 - topic_updates with shadow_indexing
    static constexpr int8_t version = -3;
    property_update<std::optional<model::compression>> compression;
    property_update<std::optional<model::cleanup_policy_bitflags>>
      cleanup_policy_bitflags;
    property_update<std::optional<model::compaction_strategy>>
      compaction_strategy;
    property_update<std::optional<model::timestamp_type>> timestamp_type;
    property_update<std::optional<size_t>> segment_size;
    property_update<tristate<size_t>> retention_bytes;
    property_update<tristate<std::chrono::milliseconds>> retention_duration;
    property_update<std::optional<model::shadow_indexing_mode>> shadow_indexing;

    auto serde_fields() {
        return std::tie(
          compression,
          cleanup_policy_bitflags,
          compaction_strategy,
          timestamp_type,
          segment_size,
          retention_bytes,
          retention_duration,
          shadow_indexing);
    }

    friend bool operator==(
      const incremental_topic_updates&, const incremental_topic_updates&)
      = default;
};

// This class contains updates for topic properties which are replicates not by
// topic_frontend
struct incremental_topic_custom_updates
  : serde::envelope<incremental_topic_custom_updates, serde::version<0>> {
    // Data-policy property is replicated by data_policy_frontend and handled by
    // data_policy_manager.
    property_update<std::optional<v8_engine::data_policy>> data_policy;

    friend bool operator==(
      const incremental_topic_custom_updates&,
      const incremental_topic_custom_updates&)
      = default;

    auto serde_fields() { return std::tie(data_policy); }
};

/**
 * Struct representing single topic properties update
 */
struct topic_properties_update
  : serde::envelope<topic_properties_update, serde::version<0>> {
    // We need version to indetify request with custom_properties
    static constexpr int32_t version = -1;
    topic_properties_update() noexcept = default;
    explicit topic_properties_update(model::topic_namespace tp_ns)
      : tp_ns(std::move(tp_ns)) {}

    topic_properties_update(
      model::topic_namespace tp_ns,
      incremental_topic_updates properties,
      incremental_topic_custom_updates custom_properties)
      : tp_ns(std::move(tp_ns))
      , properties(properties)
      , custom_properties(std::move(custom_properties)) {}

    model::topic_namespace tp_ns;

    // Tihs properties is serialized to update_topic_properties_cmd by
    // topic_frontend
    incremental_topic_updates properties;

    // This properties is not serialized to update_topic_properties_cmd, because
    // they have custom services for replication.
    incremental_topic_custom_updates custom_properties;

    friend bool
    operator==(const topic_properties_update&, const topic_properties_update&)
      = default;

    auto serde_fields() {
        return std::tie(tp_ns, properties, custom_properties);
    }
};

// Structure holding topic configuration, optionals will be replaced by broker
// defaults
struct topic_configuration
  : serde::envelope<topic_configuration, serde::version<0>> {
    topic_configuration(
      model::ns ns,
      model::topic topic,
      int32_t partition_count,
      int16_t replication_factor);

    topic_configuration() = default;

    storage::ntp_config make_ntp_config(
      const ss::sstring&,
      model::partition_id,
      model::revision_id,
      model::initial_revision_id) const;

    bool is_internal() const {
        return tp_ns.ns == model::kafka_internal_namespace
               || tp_ns == model::kafka_consumer_offsets_nt;
    }
    bool is_read_replica() const {
        return properties.read_replica && properties.read_replica.value();
    }

    model::topic_namespace tp_ns;
    // using signed integer because Kafka protocol defines it as signed int
    int32_t partition_count;
    // using signed integer because Kafka protocol defines it as signed int
    int16_t replication_factor;

    topic_properties properties;

    auto serde_fields() {
        return std::tie(tp_ns, partition_count, replication_factor, properties);
    }

    friend std::ostream& operator<<(std::ostream&, const topic_configuration&);

    friend bool
    operator==(const topic_configuration&, const topic_configuration&)
      = default;
};

struct custom_partition_assignment {
    model::partition_id id;
    std::vector<model::node_id> replicas;
    friend std::ostream&
    operator<<(std::ostream&, const custom_partition_assignment&);
};
/**
 * custom_assignable_topic_configuration type represents topic configuration
 * together with possible custom partition assignments. When assignments vector
 * is empty all the partitions will be assigned automatically.
 */
struct custom_assignable_topic_configuration {
    explicit custom_assignable_topic_configuration(topic_configuration cfg)
      : cfg(std::move(cfg)){};

    topic_configuration cfg;
    std::vector<custom_partition_assignment> custom_assignments;

    bool has_custom_assignment() const { return !custom_assignments.empty(); }
    bool is_read_replica() const { return cfg.is_read_replica(); }

    friend std::ostream&
    operator<<(std::ostream&, const custom_assignable_topic_configuration&);
};

struct create_partitions_configuration
  : serde::envelope<create_partitions_configuration, serde::version<0>> {
    using custom_assignment = std::vector<model::node_id>;

    create_partitions_configuration() = default;
    create_partitions_configuration(model::topic_namespace, int32_t);

    model::topic_namespace tp_ns;

    // This is new total number of partitions in topic.
    int32_t new_total_partition_count;

    // TODO: use when we will start supporting custom partitions assignment
    std::vector<custom_assignment> custom_assignments;

    friend bool operator==(
      const create_partitions_configuration&,
      const create_partitions_configuration&)
      = default;

    auto serde_fields() {
        return std::tie(tp_ns, new_total_partition_count, custom_assignments);
    }

    friend std::ostream&
    operator<<(std::ostream&, const create_partitions_configuration&);
};

struct topic_configuration_assignment
  : serde::envelope<topic_configuration_assignment, serde::version<0>> {
    topic_configuration_assignment() = default;

    topic_configuration_assignment(
      topic_configuration cfg, std::vector<partition_assignment> pas)
      : cfg(std::move(cfg))
      , assignments(std::move(pas)) {}

    topic_configuration cfg;
    std::vector<partition_assignment> assignments;

    model::topic_metadata get_metadata() const;

    auto serde_fields() { return std::tie(cfg, assignments); }

    friend bool operator==(
      const topic_configuration_assignment&,
      const topic_configuration_assignment&)
      = default;
};

struct create_partitions_configuration_assignment
  : serde::
      envelope<create_partitions_configuration_assignment, serde::version<0>> {
    create_partitions_configuration_assignment() = default;
    create_partitions_configuration_assignment(
      create_partitions_configuration cfg,
      std::vector<partition_assignment> pas)
      : cfg(std::move(cfg))
      , assignments(std::move(pas)) {}

    create_partitions_configuration cfg;
    std::vector<partition_assignment> assignments;

    auto serde_fields() { return std::tie(cfg, assignments); }

    friend std::ostream& operator<<(
      std::ostream&, const create_partitions_configuration_assignment&);

    friend bool operator==(
      const create_partitions_configuration_assignment&,
      const create_partitions_configuration_assignment&)
      = default;
};

struct topic_result : serde::envelope<topic_result, serde::version<0>> {
    topic_result() noexcept = default;
    explicit topic_result(model::topic_namespace t, errc ec = errc::success)
      : tp_ns(std::move(t))
      , ec(ec) {}
    model::topic_namespace tp_ns;
    errc ec;

    friend bool operator==(const topic_result&, const topic_result&) = default;

    friend std::ostream& operator<<(std::ostream& o, const topic_result& r);

    auto serde_fields() { return std::tie(tp_ns, ec); }
};

struct create_topics_request {
    std::vector<topic_configuration> topics;
    model::timeout_clock::duration timeout;
};

struct create_topics_reply {
    std::vector<topic_result> results;
    std::vector<model::topic_metadata> metadata;
    std::vector<topic_configuration> configs;
};

struct finish_partition_update_request
  : serde::envelope<finish_partition_update_request, serde::version<0>> {
    model::ntp ntp;
    std::vector<model::broker_shard> new_replica_set;

    friend bool operator==(
      const finish_partition_update_request&,
      const finish_partition_update_request&)
      = default;

    auto serde_fields() { return std::tie(ntp, new_replica_set); }
};

struct finish_partition_update_reply
  : serde::envelope<finish_partition_update_reply, serde::version<0>> {
    cluster::errc result;

    friend bool operator==(
      const finish_partition_update_reply&,
      const finish_partition_update_reply&)
      = default;

    auto serde_fields() { return std::tie(result); }
};

struct update_topic_properties_request
  : serde::envelope<update_topic_properties_request, serde::version<0>> {
    std::vector<topic_properties_update> updates;

    friend bool operator==(
      const update_topic_properties_request&,
      const update_topic_properties_request&)
      = default;

    auto serde_fields() { return std::tie(updates); }
};

struct update_topic_properties_reply
  : serde::envelope<update_topic_properties_reply, serde::version<0>> {
    std::vector<topic_result> results;

    friend bool operator==(
      const update_topic_properties_reply&,
      const update_topic_properties_reply&)
      = default;

    auto serde_fields() { return std::tie(results); }
};

template<typename T>
struct patch {
    std::vector<T> additions;
    std::vector<T> deletions;
    std::vector<T> updates;

    bool empty() const {
        return additions.empty() && deletions.empty() && updates.empty();
    }
};

// generic type used for various registration handles such as in ntp_callbacks.h
using notification_id_type = named_type<int32_t, struct notification_id>;
constexpr notification_id_type notification_id_type_invalid{-1};

struct configuration_invariants {
    static constexpr uint8_t current_version = 0;
    // version 0: node_id, core_count
    explicit configuration_invariants(model::node_id nid, uint16_t core_count)
      : node_id(nid)
      , core_count(core_count) {}

    uint8_t version = current_version;

    model::node_id node_id;
    uint16_t core_count;

    friend std::ostream&
    operator<<(std::ostream&, const configuration_invariants&);
};

class configuration_invariants_changed final : public std::exception {
public:
    explicit configuration_invariants_changed(
      const configuration_invariants& expected,
      const configuration_invariants& current)
      : _msg(ssx::sformat(
        "Configuration invariants changed. Expected: {}, current: {}",
        expected,
        current)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};
// delta propagated to backend
struct topic_table_delta {
    enum class op_type {
        add,
        del,
        update,
        update_finished,
        update_properties,
        add_non_replicable,
        del_non_replicable
    };

    topic_table_delta(
      model::ntp,
      cluster::partition_assignment,
      model::offset,
      op_type,
      std::optional<partition_assignment> = std::nullopt);

    model::ntp ntp;
    cluster::partition_assignment new_assignment;
    model::offset offset;
    op_type type;
    std::optional<partition_assignment> previous_assignment;

    model::topic_namespace_view tp_ns() const {
        return model::topic_namespace_view(ntp);
    }

    friend std::ostream& operator<<(std::ostream&, const topic_table_delta&);
    friend std::ostream& operator<<(std::ostream&, const op_type&);
};

struct create_acls_cmd_data
  : serde::envelope<create_acls_cmd_data, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding> bindings;

    friend bool
    operator==(const create_acls_cmd_data&, const create_acls_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(bindings); }
};

struct create_acls_request
  : serde::envelope<create_acls_request, serde::version<0>> {
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

    void serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        data = read_nested<create_acls_cmd_data>(in, h._bytes_left_limit);
        timeout = std::chrono::duration_cast<model::timeout_clock::duration>(
          read_nested<std::chrono::milliseconds>(in, h._bytes_left_limit));
    }

    void serde_write(iobuf& out) {
        using serde::write;
        write(out, data);
        write(
          out, std::chrono::duration_cast<std::chrono::milliseconds>(timeout));
    }
};

struct create_acls_reply
  : serde::envelope<create_acls_reply, serde::version<0>> {
    std::vector<errc> results;

    friend bool operator==(const create_acls_reply&, const create_acls_reply&)
      = default;

    auto serde_fields() { return std::tie(results); }
};

struct delete_acls_cmd_data
  : serde::envelope<delete_acls_cmd_data, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<security::acl_binding_filter> filters;

    friend bool
    operator==(const delete_acls_cmd_data&, const delete_acls_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(filters); }
};

// result for a single filter
struct delete_acls_result {
    errc error;
    std::vector<security::acl_binding> bindings;
};

struct delete_acls_request {
    delete_acls_cmd_data data;
    model::timeout_clock::duration timeout;
};

struct delete_acls_reply {
    std::vector<delete_acls_result> results;
};

struct backend_operation
  : serde::envelope<backend_operation, serde::version<0>> {
    ss::shard_id source_shard;
    partition_assignment p_as;
    topic_table_delta::op_type type;
    friend std::ostream& operator<<(std::ostream&, const backend_operation&);

    friend bool operator==(const backend_operation&, const backend_operation&)
      = default;

    auto serde_fields() { return std::tie(source_shard, p_as, type); }
};

struct create_data_policy_cmd_data
  : serde::envelope<create_data_policy_cmd_data, serde::version<0>> {
    static constexpr int8_t current_version = 1; // In future dp will be vector

    auto serde_fields() { return std::tie(dp); }

    v8_engine::data_policy dp;

    friend bool operator==(
      const create_data_policy_cmd_data&, const create_data_policy_cmd_data&)
      = default;
};

struct non_replicable_topic
  : serde::envelope<non_replicable_topic, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    model::topic_namespace source;
    model::topic_namespace name;

    friend bool
    operator==(const non_replicable_topic&, const non_replicable_topic&)
      = default;

    auto serde_fields() { return std::tie(source, name); }

    friend std::ostream& operator<<(std::ostream&, const non_replicable_topic&);
};

using config_version = named_type<int64_t, struct config_version_type>;
constexpr config_version config_version_unset = config_version{-1};

struct config_status : serde::envelope<config_status, serde::version<0>> {
    model::node_id node;
    config_version version{config_version_unset};
    bool restart{false};
    std::vector<ss::sstring> unknown;
    std::vector<ss::sstring> invalid;

    auto serde_fields() {
        return std::tie(node, version, restart, unknown, invalid);
    }

    bool operator==(const config_status&) const;
    friend std::ostream& operator<<(std::ostream&, const config_status&);
};

struct cluster_property_kv
  : serde::envelope<cluster_property_kv, serde::version<0>> {
    cluster_property_kv() = default;
    cluster_property_kv(ss::sstring k, ss::sstring v)
      : key(std::move(k))
      , value(std::move(v)) {}

    ss::sstring key;
    ss::sstring value;

    auto serde_fields() { return std::tie(key, value); }

    friend bool
    operator==(const cluster_property_kv&, const cluster_property_kv&)
      = default;
};

struct cluster_config_delta_cmd_data
  : serde::envelope<cluster_config_delta_cmd_data, serde::version<0>> {
    static constexpr int8_t current_version = 0;
    std::vector<cluster_property_kv> upsert;
    std::vector<ss::sstring> remove;

    friend bool operator==(
      const cluster_config_delta_cmd_data&,
      const cluster_config_delta_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(upsert, remove); }

    friend std::ostream&
    operator<<(std::ostream&, const cluster_config_delta_cmd_data&);
};

struct cluster_config_status_cmd_data
  : serde::envelope<cluster_config_status_cmd_data, serde::version<0>> {
    static constexpr int8_t current_version = 0;

    friend bool operator==(
      const cluster_config_status_cmd_data&,
      const cluster_config_status_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(status); }

    config_status status;
};

struct feature_update_action
  : serde::envelope<feature_update_action, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    enum class action_t : std::uint16_t {
        // Notify when a feature is done with preparing phase
        complete_preparing = 1,
        // Notify when a feature is made available, either by an administrator
        // or via auto-activation policy
        activate = 2,
        // Notify when a feature is explicitly disabled by an administrator
        deactivate = 3
    };

    // Features have an internal bitflag representation, but it is not
    // meant to be stable for use on the wire, so we refer to features by name
    ss::sstring feature_name;
    action_t action;

    friend bool
    operator==(const feature_update_action&, const feature_update_action&)
      = default;

    auto serde_fields() { return std::tie(feature_name, action); }

    friend std::ostream&
    operator<<(std::ostream&, const feature_update_action&);
};

struct feature_update_cmd_data
  : serde::envelope<feature_update_cmd_data, serde::version<0>> {
    // To avoid ambiguity on 'versions' here: `current_version`
    // is the encoding version of the struct, subsequent version
    // fields are the payload.
    static constexpr int8_t current_version = 1;

    cluster_version logical_version;
    std::vector<feature_update_action> actions;

    friend bool
    operator==(const feature_update_cmd_data&, const feature_update_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(logical_version, actions); }

    friend std::ostream&
    operator<<(std::ostream&, const feature_update_cmd_data&);
};

enum class reconciliation_status : int8_t {
    done,
    in_progress,
    error,
};
std::ostream& operator<<(std::ostream&, const reconciliation_status&);

class ntp_reconciliation_state
  : public serde::envelope<ntp_reconciliation_state, serde::version<0>> {
public:
    ntp_reconciliation_state() noexcept = default;

    // success case
    ntp_reconciliation_state(
      model::ntp, std::vector<backend_operation>, reconciliation_status);

    // error
    ntp_reconciliation_state(model::ntp, cluster::errc);

    ntp_reconciliation_state(
      model::ntp,
      std::vector<backend_operation>,
      reconciliation_status,
      cluster::errc);

    const model::ntp& ntp() const { return _ntp; }
    const std::vector<backend_operation>& pending_operations() const {
        return _backend_operations;
    }
    reconciliation_status status() const { return _status; }

    std::error_code error() const { return make_error_code(_error); }
    errc cluster_errc() const { return _error; }

    friend bool
    operator==(const ntp_reconciliation_state&, const ntp_reconciliation_state&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const ntp_reconciliation_state&);

    auto serde_fields() {
        return std::tie(_ntp, _backend_operations, _status, _error);
    }

private:
    model::ntp _ntp;
    std::vector<backend_operation> _backend_operations;
    reconciliation_status _status;
    errc _error;
};

struct reconciliation_state_request
  : serde::envelope<reconciliation_state_request, serde::version<0>> {
    std::vector<model::ntp> ntps;

    friend bool operator==(
      const reconciliation_state_request&, const reconciliation_state_request&)
      = default;

    auto serde_fields() { return std::tie(ntps); }
};

struct reconciliation_state_reply
  : serde::envelope<reconciliation_state_reply, serde::version<0>> {
    std::vector<ntp_reconciliation_state> results;

    friend bool operator==(
      const reconciliation_state_reply&, const reconciliation_state_reply&)
      = default;

    auto serde_fields() { return std::tie(results); }
};

struct decommission_node_request {
    model::node_id id;
};

struct decommission_node_reply {
    errc error;
};
struct recommission_node_request {
    model::node_id id;
};

struct recommission_node_reply {
    errc error;
};

struct finish_reallocation_request {
    model::node_id id;
};

struct finish_reallocation_reply {
    errc error;
};

struct set_maintenance_mode_request
  : serde::envelope<set_maintenance_mode_request, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    model::node_id id;
    bool enabled;

    friend bool operator==(
      const set_maintenance_mode_request&, const set_maintenance_mode_request&)
      = default;

    auto serde_fields() { return std::tie(id, enabled); }
};

struct set_maintenance_mode_reply
  : serde::envelope<set_maintenance_mode_reply, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    errc error;

    friend bool operator==(
      const set_maintenance_mode_reply&, const set_maintenance_mode_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }
};

struct config_status_request
  : serde::envelope<config_status_request, serde::version<0>> {
    config_status status;

    friend bool
    operator==(const config_status_request&, const config_status_request&)
      = default;

    auto serde_fields() { return std::tie(status); }
};

struct config_status_reply
  : serde::envelope<config_status_reply, serde::version<0>> {
    errc error;

    friend bool
    operator==(const config_status_reply&, const config_status_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }
};

struct feature_action_request {
    feature_update_action action;
};

struct feature_action_response {
    errc error;
};

using feature_barrier_tag
  = named_type<ss::sstring, struct feature_barrier_tag_type>;

struct feature_barrier_request
  : serde::envelope<feature_barrier_request, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    feature_barrier_tag tag; // Each cooperative barrier must use a unique tag
    model::node_id peer;
    bool entered; // Has the requester entered?

    friend bool
    operator==(const feature_barrier_request&, const feature_barrier_request&)
      = default;

    auto serde_fields() { return std::tie(tag, peer, entered); }
};

struct feature_barrier_response
  : serde::envelope<feature_barrier_response, serde::version<0>> {
    static constexpr int8_t current_version = 1;
    bool entered;  // Has the respondent entered?
    bool complete; // Has the respondent exited?

    friend bool
    operator==(const feature_barrier_response&, const feature_barrier_response&)
      = default;

    auto serde_fields() { return std::tie(entered, complete); }
};

struct create_non_replicable_topics_request {
    static constexpr int8_t current_version = 1;
    std::vector<non_replicable_topic> topics;
    model::timeout_clock::duration timeout;
};

struct create_non_replicable_topics_reply {
    static constexpr int8_t current_version = 1;
    std::vector<topic_result> results;
};

struct config_update_request final
  : serde::envelope<config_update_request, serde::version<0>> {
    std::vector<cluster_property_kv> upsert;
    std::vector<ss::sstring> remove;

    friend bool
    operator==(const config_update_request&, const config_update_request&)
      = default;

    auto serde_fields() { return std::tie(upsert, remove); }
};

struct config_update_reply
  : serde::envelope<config_update_reply, serde::version<0>> {
    errc error;
    cluster::config_version latest_version{config_version_unset};

    friend bool
    operator==(const config_update_reply&, const config_update_reply&)
      = default;

    auto serde_fields() { return std::tie(error, latest_version); }
};

struct hello_request final : serde::envelope<hello_request, serde::version<0>> {
    model::node_id peer;

    // milliseconds since epoch
    std::chrono::milliseconds start_time;

    friend bool operator==(const hello_request&, const hello_request&)
      = default;

    auto serde_fields() { return std::tie(peer, start_time); }
};

struct hello_reply : serde::envelope<hello_reply, serde::version<0>> {
    errc error;

    friend bool operator==(const hello_reply&, const hello_reply&) = default;

    auto serde_fields() { return std::tie(error); }
};

struct leader_term {
    leader_term(std::optional<model::node_id> leader, model::term_id term)
      : leader(leader)
      , term(term) {}

    std::optional<model::node_id> leader;
    model::term_id term;
    friend std::ostream& operator<<(std::ostream&, const leader_term&);
};

struct partition_assignment_cmp {
    using is_transparent = void;
    constexpr bool operator()(
      const partition_assignment& lhs, const partition_assignment& rhs) const {
        return lhs.id < rhs.id;
    }

    constexpr bool operator()(
      const model::partition_id& id, const partition_assignment& rhs) const {
        return id < rhs.id;
    }
    constexpr bool operator()(
      const partition_assignment& lhs, const model::partition_id& id) const {
        return lhs.id < id;
    }
    constexpr bool operator()(
      const model::partition_id& lhs, const model::partition_id& rhs) const {
        return lhs < rhs;
    }
};

using assignments_set
  = absl::btree_set<partition_assignment, partition_assignment_cmp>;

class topic_metadata {
public:
    topic_metadata(
      topic_configuration_assignment,
      model::revision_id,
      std::optional<model::initial_revision_id> = std::nullopt) noexcept;

    topic_metadata(
      topic_configuration,
      assignments_set,
      model::revision_id,
      model::topic,
      std::optional<model::initial_revision_id> = std::nullopt) noexcept;

    bool is_topic_replicable() const;
    model::revision_id get_revision() const;
    std::optional<model::initial_revision_id> get_remote_revision() const;
    const model::topic& get_source_topic() const;

    const topic_configuration& get_configuration() const;
    topic_configuration& get_configuration();

    const assignments_set& get_assignments() const;
    assignments_set& get_assignments();

private:
    topic_configuration _configuration;
    assignments_set _assignments;
    std::optional<model::topic> _source_topic;
    model::revision_id _revision;
    std::optional<model::initial_revision_id> _remote_revision;
};

} // namespace cluster
namespace std {
template<>
struct is_error_code_enum<cluster::tx_errc> : true_type {};
} // namespace std

namespace reflection {

template<>
struct adl<model::timeout_clock::duration> {
    using rep = model::timeout_clock::rep;
    using duration = model::timeout_clock::duration;

    void to(iobuf& out, duration dur);
    duration from(iobuf_parser& in);
};

template<>
struct adl<cluster::topic_configuration> {
    void to(iobuf&, cluster::topic_configuration&&);
    cluster::topic_configuration from(iobuf_parser&);
};

template<>
struct adl<cluster::join_request> {
    void to(iobuf&, cluster::join_request&&);
    cluster::join_request from(iobuf);
    cluster::join_request from(iobuf_parser&);
};

template<>
struct adl<cluster::join_reply> {
    void to(iobuf& out, cluster::join_reply&& r) { serialize(out, r.success); }
    cluster::join_reply from(iobuf_parser& in) {
        auto success = adl<bool>{}.from(in);
        return cluster::join_reply{success};
    }
};

template<>
struct adl<cluster::join_node_request> {
    void to(iobuf&, cluster::join_node_request&&);
    cluster::join_node_request from(iobuf);
    cluster::join_node_request from(iobuf_parser&);
};

template<>
struct adl<cluster::join_node_reply> {
    void to(iobuf& out, cluster::join_node_reply&& r) {
        serialize(out, r.success, r.id);
    }
    cluster::join_node_reply from(iobuf_parser& in) {
        auto success = adl<bool>{}.from(in);
        auto id = adl<model::node_id>{}.from(in);
        return {success, id};
    }
};

template<>
struct adl<cluster::configuration_update_request> {
    void to(iobuf&, cluster::configuration_update_request&&);
    cluster::configuration_update_request from(iobuf_parser&);
};

template<>
struct adl<cluster::configuration_update_reply> {
    void to(iobuf&, cluster::configuration_update_reply&&);
    cluster::configuration_update_reply from(iobuf_parser&);
};

template<>
struct adl<cluster::topic_result> {
    void to(iobuf&, cluster::topic_result&&);
    cluster::topic_result from(iobuf_parser&);
};

template<>
struct adl<cluster::create_topics_request> {
    void to(iobuf&, cluster::create_topics_request&&);
    cluster::create_topics_request from(iobuf);
    cluster::create_topics_request from(iobuf_parser&);
};

template<>
struct adl<cluster::create_non_replicable_topics_request> {
    void to(iobuf&, cluster::create_non_replicable_topics_request&&);
    cluster::create_non_replicable_topics_request from(iobuf_parser&);
};

template<>
struct adl<cluster::create_non_replicable_topics_reply> {
    void to(iobuf&, cluster::create_non_replicable_topics_reply&&);
    cluster::create_non_replicable_topics_reply from(iobuf_parser&);
};

template<>
struct adl<cluster::create_topics_reply> {
    void to(iobuf&, cluster::create_topics_reply&&);
    cluster::create_topics_reply from(iobuf);
    cluster::create_topics_reply from(iobuf_parser&);
};
template<>
struct adl<cluster::topic_configuration_assignment> {
    void to(iobuf&, cluster::topic_configuration_assignment&&);
    cluster::topic_configuration_assignment from(iobuf_parser&);
};

template<>
struct adl<cluster::configuration_invariants> {
    void to(iobuf&, cluster::configuration_invariants&&);
    cluster::configuration_invariants from(iobuf_parser&);
};

template<>
struct adl<cluster::topic_properties_update> {
    void to(iobuf&, cluster::topic_properties_update&&);
    cluster::topic_properties_update from(iobuf_parser&);
};
template<>
struct adl<cluster::ntp_reconciliation_state> {
    void to(iobuf&, cluster::ntp_reconciliation_state&&);
    cluster::ntp_reconciliation_state from(iobuf_parser&);
};

template<>
struct adl<cluster::create_acls_cmd_data> {
    void to(iobuf&, cluster::create_acls_cmd_data&&);
    cluster::create_acls_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::delete_acls_cmd_data> {
    void to(iobuf&, cluster::delete_acls_cmd_data&&);
    cluster::delete_acls_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::delete_acls_result> {
    void to(iobuf&, cluster::delete_acls_result&&);
    cluster::delete_acls_result from(iobuf_parser&);
};

template<>
struct adl<cluster::create_partitions_configuration> {
    void to(iobuf&, cluster::create_partitions_configuration&&);
    cluster::create_partitions_configuration from(iobuf_parser&);
};

template<>
struct adl<cluster::create_partitions_configuration_assignment> {
    void to(iobuf&, cluster::create_partitions_configuration_assignment&&);
    cluster::create_partitions_configuration_assignment from(iobuf_parser&);
};

template<>
struct adl<cluster::create_data_policy_cmd_data> {
    void to(iobuf&, cluster::create_data_policy_cmd_data&&);
    cluster::create_data_policy_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::non_replicable_topic> {
    void to(iobuf& out, cluster::non_replicable_topic&&);
    cluster::non_replicable_topic from(iobuf_parser&);
};

template<>
struct adl<cluster::incremental_topic_updates> {
    void to(iobuf& out, cluster::incremental_topic_updates&&);
    cluster::incremental_topic_updates from(iobuf_parser&);
};

template<>
struct adl<cluster::config_status> {
    void to(iobuf& out, cluster::config_status&&);
    cluster::config_status from(iobuf_parser&);
};

template<>
struct adl<cluster::cluster_config_delta_cmd_data> {
    void to(iobuf& out, cluster::cluster_config_delta_cmd_data&&);
    cluster::cluster_config_delta_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::cluster_config_status_cmd_data> {
    void to(iobuf& out, cluster::cluster_config_status_cmd_data&&);
    cluster::cluster_config_status_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_update_action> {
    void to(iobuf& out, cluster::feature_update_action&&);
    cluster::feature_update_action from(iobuf_parser&);
};

template<>
struct adl<cluster::incremental_topic_custom_updates> {
    void to(iobuf& out, cluster::incremental_topic_custom_updates&&);
    cluster::incremental_topic_custom_updates from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_update_cmd_data> {
    void to(iobuf&, cluster::feature_update_cmd_data&&);
    cluster::feature_update_cmd_data from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_barrier_request> {
    void to(iobuf&, cluster::feature_barrier_request&&);
    cluster::feature_barrier_request from(iobuf_parser&);
};

template<>
struct adl<cluster::feature_barrier_response> {
    void to(iobuf&, cluster::feature_barrier_response&&);
    cluster::feature_barrier_response from(iobuf_parser&);
};

template<>
struct adl<cluster::set_maintenance_mode_request> {
    void to(iobuf&, cluster::set_maintenance_mode_request&&);
    cluster::set_maintenance_mode_request from(iobuf_parser&);
};

template<>
struct adl<cluster::set_maintenance_mode_reply> {
    void to(iobuf&, cluster::set_maintenance_mode_reply&&);
    cluster::set_maintenance_mode_reply from(iobuf_parser&);
};

template<>
struct adl<cluster::allocate_id_request> {
    void to(iobuf& out, cluster::allocate_id_request&& r) {
        serialize(out, r.timeout);
    }
    cluster::allocate_id_request from(iobuf_parser& in) {
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return cluster::allocate_id_request(timeout);
    }
};

template<>
struct adl<cluster::allocate_id_reply> {
    void to(iobuf& out, cluster::allocate_id_reply&& r) {
        serialize(out, r.id, r.ec);
    }
    cluster::allocate_id_reply from(iobuf_parser& in) {
        auto id = adl<int64_t>{}.from(in);
        auto ec = adl<cluster::errc>{}.from(in);
        return {id, ec};
    }
};
template<>
struct adl<cluster::partition_assignment> {
    void to(iobuf&, cluster::partition_assignment&&);
    cluster::partition_assignment from(iobuf_parser&);
};

template<>
struct adl<cluster::remote_topic_properties> {
    void to(iobuf&, cluster::remote_topic_properties&&);
    cluster::remote_topic_properties from(iobuf_parser&);
};

template<>
struct adl<cluster::topic_properties> {
    void to(iobuf&, cluster::topic_properties&&);
    cluster::topic_properties from(iobuf_parser&);
};

template<typename T>
struct adl<cluster::property_update<T>> {
    void to(iobuf& out, cluster::property_update<T>&& update) {
        reflection::serialize(out, std::move(update.value), update.op);
    }

    cluster::property_update<T> from(iobuf_parser& parser) {
        auto value = reflection::adl<T>{}.from(parser);
        auto op = reflection::adl<cluster::incremental_update_operation>{}.from(
          parser);
        return {std::move(value), op};
    }
};
template<>
struct adl<cluster::cluster_property_kv> {
    void to(iobuf&, cluster::cluster_property_kv&&);
    cluster::cluster_property_kv from(iobuf_parser&);
};

template<>
struct adl<cluster::abort_group_tx_request> {
    void to(iobuf& out, cluster::abort_group_tx_request&& r) {
        serialize(
          out,
          std::move(r.ntp),
          std::move(r.group_id),
          r.pid,
          r.tx_seq,
          r.timeout);
    }
    cluster::abort_group_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto group_id = adl<kafka::group_id>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(ntp), std::move(group_id), pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::abort_group_tx_reply> {
    void to(iobuf& out, cluster::abort_group_tx_reply&& r) {
        serialize(out, r.ec);
    }
    cluster::abort_group_tx_reply from(iobuf_parser& in) {
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return cluster::abort_group_tx_reply{ec};
    }
};

template<>
struct adl<cluster::commit_group_tx_request> {
    void to(iobuf& out, cluster::commit_group_tx_request&& r) {
        serialize(
          out,
          std::move(r.ntp),
          r.pid,
          r.tx_seq,
          std::move(r.group_id),
          r.timeout);
    }
    cluster::commit_group_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto group_id = adl<kafka::group_id>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(ntp), pid, tx_seq, std::move(group_id), timeout};
    }
};

template<>
struct adl<cluster::commit_group_tx_reply> {
    void to(iobuf& out, cluster::commit_group_tx_reply&& r) {
        serialize(out, r.ec);
    }
    cluster::commit_group_tx_reply from(iobuf_parser& in) {
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return cluster::commit_group_tx_reply{ec};
    }
};

template<>
struct adl<cluster::prepare_group_tx_request> {
    void to(iobuf& out, cluster::prepare_group_tx_request&& r) {
        serialize(
          out,
          std::move(r.ntp),
          std::move(r.group_id),
          r.etag,
          r.pid,
          r.tx_seq,
          r.timeout);
    }
    cluster::prepare_group_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto group_id = adl<kafka::group_id>{}.from(in);
        auto etag = adl<model::term_id>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {
          std::move(ntp), std::move(group_id), etag, pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::prepare_group_tx_reply> {
    void to(iobuf& out, cluster::prepare_group_tx_reply&& r) {
        serialize(out, r.ec);
    }
    cluster::prepare_group_tx_reply from(iobuf_parser& in) {
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return cluster::prepare_group_tx_reply{ec};
    }
};

template<>
struct adl<cluster::begin_group_tx_request> {
    void to(iobuf& out, cluster::begin_group_tx_request&& r) {
        serialize(
          out,
          std::move(r.ntp),
          std::move(r.group_id),
          r.pid,
          r.tx_seq,
          r.timeout);
    }
    cluster::begin_group_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto group_id = adl<kafka::group_id>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(ntp), std::move(group_id), pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::begin_group_tx_reply> {
    void to(iobuf& out, cluster::begin_group_tx_reply&& r) {
        serialize(out, r.etag, r.ec);
    }
    cluster::begin_group_tx_reply from(iobuf_parser& in) {
        auto etag = adl<model::term_id>{}.from(in);
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return {etag, ec};
    }
};

template<>
struct adl<cluster::abort_tx_request> {
    void to(iobuf& out, cluster::abort_tx_request&& r) {
        serialize(out, std::move(r.ntp), r.pid, r.tx_seq, r.timeout);
    }
    cluster::abort_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(ntp), pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::abort_tx_reply> {
    void to(iobuf& out, cluster::abort_tx_reply&& r) { serialize(out, r.ec); }
    cluster::abort_tx_reply from(iobuf_parser& in) {
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return cluster::abort_tx_reply{ec};
    }
};

template<>
struct adl<cluster::commit_tx_request> {
    void to(iobuf& out, cluster::commit_tx_request&& r) {
        serialize(out, std::move(r.ntp), r.pid, r.tx_seq, r.timeout);
    }
    cluster::commit_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(ntp), pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::commit_tx_reply> {
    void to(iobuf& out, cluster::commit_tx_reply&& r) { serialize(out, r.ec); }
    cluster::commit_tx_reply from(iobuf_parser& in) {
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return cluster::commit_tx_reply{ec};
    }
};

template<>
struct adl<cluster::prepare_tx_request> {
    void to(iobuf& out, cluster::prepare_tx_request&& r) {
        serialize(
          out, std::move(r.ntp), r.etag, r.tm, r.pid, r.tx_seq, r.timeout);
    }
    cluster::prepare_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto etag = adl<model::term_id>{}.from(in);
        auto tm = adl<model::partition_id>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(ntp), etag, tm, pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::prepare_tx_reply> {
    void to(iobuf& out, cluster::prepare_tx_reply&& r) { serialize(out, r.ec); }
    cluster::prepare_tx_reply from(iobuf_parser& in) {
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return cluster::prepare_tx_reply{ec};
    }
};

template<>
struct adl<cluster::begin_tx_request> {
    void to(iobuf& out, cluster::begin_tx_request&& r) {
        serialize(
          out, std::move(r.ntp), r.pid, r.tx_seq, r.transaction_timeout_ms);
    }
    cluster::begin_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<std::chrono::milliseconds>{}.from(in);
        return {std::move(ntp), pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::begin_tx_reply> {
    void to(iobuf& out, cluster::begin_tx_reply&& r) {
        serialize(out, std::move(r.ntp), r.etag, r.ec);
    }
    cluster::begin_tx_reply from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto etag = adl<model::term_id>{}.from(in);
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return {std::move(ntp), etag, ec};
    }
};

template<>
struct adl<cluster::try_abort_request> {
    void to(iobuf& out, cluster::try_abort_request&& r) {
        serialize(out, r.tm, r.pid, r.tx_seq, r.timeout);
    }
    cluster::try_abort_request from(iobuf_parser& in) {
        auto tm = adl<model::partition_id>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {tm, pid, tx_seq, timeout};
    }
};

template<>
struct adl<cluster::try_abort_reply> {
    void to(iobuf& out, cluster::try_abort_reply&& r) {
        serialize(out, bool(r.commited), bool(r.aborted), r.ec);
    }
    cluster::try_abort_reply from(iobuf_parser& in) {
        using committed_type = cluster::try_abort_reply::committed_type;
        using aborted_type = cluster::try_abort_reply::aborted_type;
        auto committed = committed_type(adl<bool>{}.from(in));
        auto aborted = aborted_type(adl<bool>{}.from(in));
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return {committed, aborted, ec};
    }
};

template<>
struct adl<cluster::init_tm_tx_request> {
    void to(iobuf& out, cluster::init_tm_tx_request&& r) {
        serialize(out, std::move(r.tx_id), r.transaction_timeout_ms, r.timeout);
    }
    cluster::init_tm_tx_request from(iobuf_parser& in) {
        auto tx_id = adl<kafka::transactional_id>{}.from(in);
        auto tx_timeout = adl<std::chrono::milliseconds>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(tx_id), tx_timeout, timeout};
    }
};

template<>
struct adl<cluster::init_tm_tx_reply> {
    void to(iobuf& out, cluster::init_tm_tx_reply&& r) {
        serialize(out, r.pid, r.ec);
    }
    cluster::init_tm_tx_reply from(iobuf_parser& in) {
        auto pid = adl<model::producer_identity>{}.from(in);
        auto ec = adl<cluster::tx_errc>{}.from(in);
        return {pid, ec};
    }
};

template<>
struct adl<cluster::config_update_request> {
    void to(iobuf& out, cluster::config_update_request&& r) {
        serialize(out, r.upsert, r.remove);
    }
    cluster::config_update_request from(iobuf_parser& in) {
        auto upsert = adl<std::vector<cluster::cluster_property_kv>>{}.from(in);
        auto remove = adl<std::vector<ss::sstring>>{}.from(in);
        return {.upsert = upsert, .remove = remove};
    }
};

template<>
struct adl<cluster::config_update_reply> {
    void to(iobuf& out, cluster::config_update_reply&& r) {
        serialize(out, r.error, r.latest_version);
    }
    cluster::config_update_reply from(iobuf_parser& in) {
        auto error = adl<cluster::errc>{}.from(in);
        auto latest_version = adl<cluster::config_version>{}.from(in);
        return {.error = error, .latest_version = latest_version};
    }
};

template<>
struct adl<cluster::hello_request> {
    void to(iobuf& out, cluster::hello_request&& r) {
        serialize(out, r.peer, r.start_time);
    }
    cluster::hello_request from(iobuf_parser& in) {
        auto peer = adl<model::node_id>{}.from(in);
        auto start_time = adl<std::chrono::milliseconds>{}.from(in);
        return {.peer = peer, .start_time = start_time};
    }
};

template<>
struct adl<cluster::hello_reply> {
    void to(iobuf& out, cluster::hello_reply&& r) { serialize(out, r.error); }
    cluster::hello_reply from(iobuf_parser& in) {
        auto error = adl<cluster::errc>{}.from(in);
        return {.error = error};
    }
};

template<>
struct adl<cluster::config_status_request> {
    void to(iobuf& out, cluster::config_status_request&& r) {
        serialize(out, r.status);
    }
    cluster::config_status_request from(iobuf_parser& in) {
        auto status = adl<cluster::config_status>{}.from(in);
        return {.status = status};
    }
};

template<>
struct adl<cluster::config_status_reply> {
    void to(iobuf& out, cluster::config_status_reply&& r) {
        serialize(out, r.error);
    }
    cluster::config_status_reply from(iobuf_parser& in) {
        auto error = adl<cluster::errc>{}.from(in);
        return {.error = error};
    }
};

template<>
struct adl<cluster::finish_partition_update_request> {
    void to(iobuf& out, cluster::finish_partition_update_request&& r) {
        serialize(out, std::move(r.ntp), std::move(r.new_replica_set));
    }
    cluster::finish_partition_update_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto new_replica_set = adl<std::vector<model::broker_shard>>{}.from(in);
        return {
          .ntp = std::move(ntp),
          .new_replica_set = std::move(new_replica_set),
        };
    }
};

template<>
struct adl<cluster::finish_partition_update_reply> {
    void to(iobuf& out, cluster::finish_partition_update_reply&& r) {
        serialize(out, r.result);
    }
    cluster::finish_partition_update_reply from(iobuf_parser& in) {
        auto result = adl<cluster::errc>{}.from(in);
        return {.result = result};
    }
};

template<>
struct adl<cluster::update_topic_properties_request> {
    void to(iobuf& out, cluster::update_topic_properties_request&& r) {
        serialize(out, std::move(r.updates));
    }
    cluster::update_topic_properties_request from(iobuf_parser& in) {
        auto updates
          = adl<std::vector<cluster::topic_properties_update>>{}.from(in);
        return {.updates = std::move(updates)};
    }
};

template<>
struct adl<cluster::update_topic_properties_reply> {
    void to(iobuf& out, cluster::update_topic_properties_reply&& r) {
        serialize(out, std::move(r.results));
    }
    cluster::update_topic_properties_reply from(iobuf_parser& in) {
        auto results = adl<std::vector<cluster::topic_result>>{}.from(in);
        return {.results = std::move(results)};
    }
};

template<>
struct adl<cluster::reconciliation_state_request> {
    void to(iobuf& out, cluster::reconciliation_state_request&& r) {
        serialize(out, std::move(r.ntps));
    }
    cluster::reconciliation_state_request from(iobuf_parser& in) {
        auto ntps = adl<std::vector<model::ntp>>{}.from(in);
        return {.ntps = std::move(ntps)};
    }
};

template<>
struct adl<cluster::backend_operation> {
    void to(iobuf& out, cluster::backend_operation&& r) {
        serialize(out, r.source_shard, r.p_as, r.type);
    }
    cluster::backend_operation from(iobuf_parser& in) {
        auto source_shard = adl<ss::shard_id>{}.from(in);
        auto p_as = adl<cluster::partition_assignment>{}.from(in);
        auto type = adl<cluster::topic_table_delta::op_type>{}.from(in);
        return {
          .source_shard = source_shard,
          .p_as = std::move(p_as),
          .type = type,
        };
    }
};

template<>
struct adl<cluster::reconciliation_state_reply> {
    void to(iobuf& out, cluster::reconciliation_state_reply&& r) {
        serialize(out, r.results);
    }
    cluster::reconciliation_state_reply from(iobuf_parser& in) {
        auto results
          = adl<std::vector<cluster::ntp_reconciliation_state>>{}.from(in);
        return {.results = std::move(results)};
    }
};

template<>
struct adl<cluster::create_acls_request> {
    void to(iobuf& out, cluster::create_acls_request&& r) {
        serialize(out, std::move(r.data), r.timeout);
    }
    cluster::create_acls_request from(iobuf_parser& in) {
        auto data = adl<cluster::create_acls_cmd_data>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(data), timeout};
    }
};

template<>
struct adl<cluster::create_acls_reply> {
    void to(iobuf& out, cluster::create_acls_reply&& r) {
        serialize(out, std::move(r.results));
    }
    cluster::create_acls_reply from(iobuf_parser& in) {
        auto results = adl<std::vector<cluster::errc>>{}.from(in);
        return {.results = std::move(results)};
    }
};
} // namespace reflection
