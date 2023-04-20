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
#include "security/license.h"
#include "security/scram_credential.h"
#include "security/types.h"
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

using transfer_leadership_request = raft::transfer_leadership_request;
using transfer_leadership_reply = raft::transfer_leadership_reply;

// A cluster version is a logical protocol version describing the content
// of the raft0 on disk structures, and available features.  These are
// passed over the network via the health_manager, and persisted in
// the feature_manager
using cluster_version = named_type<int64_t, struct cluster_version_tag>;
constexpr cluster_version invalid_version = cluster_version{-1};

struct allocate_id_request
  : serde::envelope<
      allocate_id_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::timeout_clock::duration timeout;

    allocate_id_request() noexcept = default;

    explicit allocate_id_request(model::timeout_clock::duration timeout)
      : timeout(timeout) {}

    friend bool
    operator==(const allocate_id_request&, const allocate_id_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const allocate_id_request& req) {
        fmt::print(o, "timeout: {}", req.timeout.count());
        return o;
    }

    auto serde_fields() { return std::tie(timeout); }
};

struct allocate_id_reply
  : serde::
      envelope<allocate_id_reply, serde::version<0>, serde::compat_version<0>> {
    int64_t id;
    errc ec;

    allocate_id_reply() noexcept = default;

    allocate_id_reply(int64_t id, errc ec)
      : id(id)
      , ec(ec) {}

    friend bool operator==(const allocate_id_reply&, const allocate_id_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const allocate_id_reply& rep) {
        fmt::print(o, "id: {}, ec: {}", rep.id, rep.ec);
        return o;
    }

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
    invalid_txn_state,
    invalid_producer_epoch,
    tx_not_found,
    tx_id_not_found
};

std::ostream& operator<<(std::ostream&, const tx_errc&);

struct tx_errc_category final : public std::error_category {
    const char* name() const noexcept final { return "cluster::tx_errc"; }

    std::string message(int) const final;
};
inline const std::error_category& tx_error_category() noexcept {
    static tx_errc_category e;
    return e;
}
inline std::error_code make_error_code(tx_errc e) noexcept {
    return std::error_code(static_cast<int>(e), tx_error_category());
}

struct kafka_result {
    kafka::offset last_offset;
};
struct kafka_stages {
    kafka_stages(ss::future<>, ss::future<result<kafka_result>>);
    explicit kafka_stages(raft::errc);
    // after this future is ready, request in enqueued in raft and it will not
    // be reorderd
    ss::future<> request_enqueued;
    // after this future is ready, request was successfully replicated with
    // requested consistency level
    ss::future<result<kafka_result>> replicate_finished;
};

/**
 * When we remove a partition in the controller backend, we need to know
 * whether the action is just for this node, or whether the partition
 * is being deleted overall.
 */
enum class partition_removal_mode : uint8_t {
    // We are removing a partition from this node only: delete
    // local data but leave remote data alone.
    local_only = 0,
    // The partition is being permanently deleted from all nodes:
    // remove remote data as well as local data.
    global = 1
};

struct try_abort_request
  : serde::
      envelope<try_abort_request, serde::version<0>, serde::compat_version<0>> {
    model::partition_id tm;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout{};

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

    friend std::ostream&
    operator<<(std::ostream& o, const try_abort_request& r);

    auto serde_fields() { return std::tie(tm, pid, tx_seq, timeout); }
};

struct try_abort_reply
  : serde::
      envelope<try_abort_reply, serde::version<0>, serde::compat_version<0>> {
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

    friend std::ostream& operator<<(std::ostream& o, const try_abort_reply& r);

    static try_abort_reply make_aborted() {
        return {committed_type::no, aborted_type::yes, tx_errc::none};
    }

    static try_abort_reply make_committed() {
        return {committed_type::yes, aborted_type::no, tx_errc::none};
    }

    auto serde_fields() { return std::tie(commited, aborted, ec); }
};

struct init_tm_tx_request
  : serde::envelope<
      init_tm_tx_request,
      serde::version<0>,
      serde::compat_version<0>> {
    kafka::transactional_id tx_id{};
    std::chrono::milliseconds transaction_timeout_ms{};
    model::timeout_clock::duration timeout{};

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

    friend std::ostream&
    operator<<(std::ostream& o, const init_tm_tx_request& r);

    auto serde_fields() {
        return std::tie(tx_id, transaction_timeout_ms, timeout);
    }
};

struct init_tm_tx_reply
  : serde::
      envelope<init_tm_tx_reply, serde::version<0>, serde::compat_version<0>> {
    // partition_not_exists, not_leader, topic_not_exists
    model::producer_identity pid;
    tx_errc ec;

    init_tm_tx_reply() noexcept = default;

    init_tm_tx_reply(model::producer_identity pid, tx_errc ec)
      : pid(pid)
      , ec(ec) {}

    friend bool operator==(const init_tm_tx_reply&, const init_tm_tx_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const init_tm_tx_reply& r);

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
struct fetch_tx_request
  : serde::
      envelope<fetch_tx_request, serde::version<1>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    kafka::transactional_id tx_id{};
    model::term_id term{};
    model::partition_id tm{0};

    fetch_tx_request() noexcept = default;

    fetch_tx_request(
      kafka::transactional_id tx_id,
      model::term_id term,
      model::partition_id tm)
      : tx_id(tx_id)
      , term(term)
      , tm(tm) {}

    friend bool operator==(const fetch_tx_request&, const fetch_tx_request&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const fetch_tx_request& r);

    auto serde_fields() { return std::tie(tx_id, term, tm); }
};

struct fetch_tx_reply
  : serde::
      envelope<fetch_tx_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    enum tx_status : int32_t {
        ongoing,
        preparing,
        prepared,
        aborting,
        killed,
        ready,
        tombstone,
    };

    struct tx_partition
      : serde::
          envelope<tx_partition, serde::version<0>, serde::compat_version<0>> {
        using rpc_adl_exempt = std::true_type;

        model::ntp ntp;
        model::term_id etag;
        model::revision_id topic_revision;

        tx_partition() noexcept = default;

        tx_partition(
          model::ntp ntp,
          model::term_id etag,
          model::revision_id topic_revision)
          : ntp(ntp)
          , etag(etag)
          , topic_revision(topic_revision) {}

        friend bool operator==(const tx_partition&, const tx_partition&)
          = default;

        friend std::ostream& operator<<(std::ostream& o, const tx_partition& r);

        auto serde_fields() { return std::tie(ntp, etag, topic_revision); }
    };

    struct tx_group
      : serde::envelope<tx_group, serde::version<0>, serde::compat_version<0>> {
        using rpc_adl_exempt = std::true_type;

        kafka::group_id group_id;
        model::term_id etag;

        tx_group() noexcept = default;

        tx_group(kafka::group_id group_id, model::term_id etag)
          : group_id(group_id)
          , etag(etag) {}

        friend bool operator==(const tx_group&, const tx_group&) = default;

        friend std::ostream& operator<<(std::ostream& o, const tx_partition& r);

        auto serde_fields() { return std::tie(group_id, etag); }
    };

    tx_errc ec{};
    model::producer_identity pid{};
    model::producer_identity last_pid{};
    model::tx_seq tx_seq{};
    std::chrono::milliseconds timeout_ms{};
    tx_status status{};
    std::vector<tx_partition> partitions{};
    std::vector<tx_group> groups{};

    fetch_tx_reply() noexcept = default;

    fetch_tx_reply(tx_errc ec)
      : ec(ec) {}

    fetch_tx_reply(
      tx_errc ec,
      model::producer_identity pid,
      model::producer_identity last_pid,
      model::tx_seq tx_seq,
      std::chrono::milliseconds timeout_ms,
      tx_status status,
      std::vector<tx_partition> partitions,
      std::vector<tx_group> groups)
      : ec(ec)
      , pid(pid)
      , last_pid(last_pid)
      , tx_seq(tx_seq)
      , timeout_ms(timeout_ms)
      , status(status)
      , partitions(partitions)
      , groups(groups) {}

    friend bool operator==(const fetch_tx_reply&, const fetch_tx_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const fetch_tx_reply& r);

    auto serde_fields() {
        return std::tie(
          ec, pid, last_pid, tx_seq, timeout_ms, status, partitions, groups);
    }
};

struct begin_tx_request
  : serde::
      envelope<begin_tx_request, serde::version<1>, serde::compat_version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    std::chrono::milliseconds transaction_timeout_ms{};
    model::partition_id tm_partition{0};

    begin_tx_request() noexcept = default;

    begin_tx_request(
      model::ntp ntp,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      std::chrono::milliseconds transaction_timeout_ms,
      model::partition_id tm_partition)
      : ntp(std::move(ntp))
      , pid(pid)
      , tx_seq(tx_seq)
      , transaction_timeout_ms(transaction_timeout_ms)
      , tm_partition(tm_partition) {}

    friend bool operator==(const begin_tx_request&, const begin_tx_request&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const begin_tx_request& r);

    auto serde_fields() {
        return std::tie(ntp, pid, tx_seq, transaction_timeout_ms, tm_partition);
    }
};

struct begin_tx_reply
  : serde::
      envelope<begin_tx_reply, serde::version<1>, serde::compat_version<0>> {
    model::ntp ntp;
    model::term_id etag;
    tx_errc ec;
    model::revision_id topic_revision = model::revision_id(-1);

    begin_tx_reply() noexcept = default;

    begin_tx_reply(
      model::ntp ntp,
      model::term_id etag,
      tx_errc ec,
      model::revision_id topic_revision)
      : ntp(std::move(ntp))
      , etag(etag)
      , ec(ec)
      , topic_revision(topic_revision) {}

    begin_tx_reply(model::ntp ntp, model::term_id etag, tx_errc ec)
      : ntp(std::move(ntp))
      , etag(etag)
      , ec(ec) {}

    begin_tx_reply(model::ntp ntp, tx_errc ec)
      : ntp(std::move(ntp))
      , ec(ec) {}

    friend bool operator==(const begin_tx_reply&, const begin_tx_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const begin_tx_reply& r);

    auto serde_fields() { return std::tie(ntp, etag, ec, topic_revision); }
};

struct prepare_tx_request
  : serde::envelope<
      prepare_tx_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    model::term_id etag;
    model::partition_id tm;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout{};

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

    friend std::ostream&
    operator<<(std::ostream& o, const prepare_tx_request& r);

    auto serde_fields() {
        return std::tie(ntp, etag, tm, pid, tx_seq, timeout);
    }
};

struct prepare_tx_reply
  : serde::
      envelope<prepare_tx_reply, serde::version<0>, serde::compat_version<0>> {
    tx_errc ec{};

    prepare_tx_reply() noexcept = default;

    explicit prepare_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool operator==(const prepare_tx_reply&, const prepare_tx_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const prepare_tx_reply& r);

    auto serde_fields() { return std::tie(ec); }
};

struct commit_tx_request
  : serde::
      envelope<commit_tx_request, serde::version<0>, serde::compat_version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout{};

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

    auto serde_fields() { return std::tie(ntp, pid, tx_seq, timeout); }

    friend std::ostream&
    operator<<(std::ostream& o, const commit_tx_request& r);
};

struct commit_tx_reply
  : serde::
      envelope<commit_tx_reply, serde::version<0>, serde::compat_version<0>> {
    tx_errc ec{};

    commit_tx_reply() noexcept = default;

    explicit commit_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool operator==(const commit_tx_reply&, const commit_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }

    friend std::ostream& operator<<(std::ostream& o, const commit_tx_reply& r);
};

struct abort_tx_request
  : serde::
      envelope<abort_tx_request, serde::version<0>, serde::compat_version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout{};

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

    auto serde_fields() { return std::tie(ntp, pid, tx_seq, timeout); }

    friend std::ostream& operator<<(std::ostream& o, const abort_tx_request& r);
};

struct abort_tx_reply
  : serde::
      envelope<abort_tx_reply, serde::version<0>, serde::compat_version<0>> {
    tx_errc ec{};

    abort_tx_reply() noexcept = default;

    explicit abort_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool operator==(const abort_tx_reply&, const abort_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }

    friend std::ostream& operator<<(std::ostream& o, const abort_tx_reply& r);
};

struct begin_group_tx_request
  : serde::envelope<
      begin_group_tx_request,
      serde::version<1>,
      serde::compat_version<0>> {
    model::ntp ntp;
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout{};
    model::partition_id tm_partition{0};

    begin_group_tx_request() noexcept = default;

    begin_group_tx_request(
      model::ntp ntp,
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout,
      model::partition_id tm)
      : ntp(std::move(ntp))
      , group_id(std::move(group_id))
      , pid(pid)
      , tx_seq(tx_seq)
      , timeout(timeout)
      , tm_partition(tm) {}

    /*
     * construct with default value model::ntp
     * https://github.com/redpanda-data/redpanda/issues/5055
     */
    begin_group_tx_request(
      kafka::group_id group_id,
      model::producer_identity pid,
      model::tx_seq tx_seq,
      model::timeout_clock::duration timeout,
      model::partition_id tm)
      : begin_group_tx_request(
        model::ntp(), std::move(group_id), pid, tx_seq, timeout, tm) {}

    friend bool
    operator==(const begin_group_tx_request&, const begin_group_tx_request&)
      = default;

    auto serde_fields() {
        return std::tie(ntp, group_id, pid, tx_seq, timeout, tm_partition);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const begin_group_tx_request& r);
};

struct begin_group_tx_reply
  : serde::envelope<
      begin_group_tx_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    model::term_id etag;
    tx_errc ec{};

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

    friend std::ostream&
    operator<<(std::ostream& o, const begin_group_tx_reply& r);
};

struct prepare_group_tx_request
  : serde::envelope<
      prepare_group_tx_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    kafka::group_id group_id;
    model::term_id etag;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout{};

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

    auto serde_fields() {
        return std::tie(ntp, group_id, etag, pid, tx_seq, timeout);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const prepare_group_tx_request& r);

    friend bool
    operator==(const prepare_group_tx_request&, const prepare_group_tx_request&)
      = default;
};

struct prepare_group_tx_reply
  : serde::envelope<
      prepare_group_tx_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    tx_errc ec{};

    prepare_group_tx_reply() noexcept = default;

    explicit prepare_group_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool
    operator==(const prepare_group_tx_reply&, const prepare_group_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }

    friend std::ostream&
    operator<<(std::ostream& o, const prepare_group_tx_reply& r);
};

struct commit_group_tx_request
  : serde::envelope<
      commit_group_tx_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    kafka::group_id group_id;
    model::timeout_clock::duration timeout{};

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

    friend std::ostream&
    operator<<(std::ostream& o, const commit_group_tx_request& r);

    auto serde_fields() {
        return std::tie(ntp, pid, tx_seq, group_id, timeout);
    }
};

struct commit_group_tx_reply
  : serde::envelope<
      commit_group_tx_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    tx_errc ec{};

    commit_group_tx_reply() noexcept = default;

    explicit commit_group_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool
    operator==(const commit_group_tx_reply&, const commit_group_tx_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const commit_group_tx_reply& r);

    auto serde_fields() { return std::tie(ec); }
};

struct abort_group_tx_request
  : serde::envelope<
      abort_group_tx_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    kafka::group_id group_id;
    model::producer_identity pid;
    model::tx_seq tx_seq;
    model::timeout_clock::duration timeout{};

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

    friend std::ostream&
    operator<<(std::ostream& o, const abort_group_tx_request& r);

    auto serde_fields() {
        return std::tie(ntp, group_id, pid, tx_seq, timeout);
    }
};

struct abort_group_tx_reply
  : serde::envelope<
      abort_group_tx_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    tx_errc ec{};

    abort_group_tx_reply() noexcept = default;

    explicit abort_group_tx_reply(tx_errc ec)
      : ec(ec) {}

    friend bool
    operator==(const abort_group_tx_reply&, const abort_group_tx_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const abort_group_tx_reply& r);

    auto serde_fields() { return std::tie(ec); }
};

/// Old-style request sent by node to join raft-0
/// - Does not specify logical version
/// - Always specifies node_id
/// (remove this RPC two versions after join_node_request was
///  added to replace it)
struct join_request
  : serde::envelope<join_request, serde::version<0>, serde::compat_version<0>> {
    join_request() noexcept = default;

    explicit join_request(model::broker b)
      : node(std::move(b)) {}

    model::broker node;

    friend bool operator==(const join_request&, const join_request&) = default;

    friend std::ostream& operator<<(std::ostream& o, const join_request& r) {
        fmt::print(o, "node {}", r.node);
        return o;
    }

    auto serde_fields() { return std::tie(node); }
};

struct join_reply
  : serde::envelope<join_reply, serde::version<0>, serde::compat_version<0>> {
    bool success;

    join_reply() noexcept = default;

    explicit join_reply(bool success)
      : success(success) {}

    friend bool operator==(const join_reply&, const join_reply&) = default;

    friend std::ostream& operator<<(std::ostream& o, const join_reply& r) {
        fmt::print(o, "success {}", r.success);
        return o;
    }

    auto serde_fields() { return std::tie(success); }
};

/// Successor to join_request:
/// - Include version metadata for joining node
/// - Has fields for implementing auto-selection of
///   node_id (https://github.com/redpanda-data/redpanda/issues/2793)
///   in future.
struct join_node_request
  : serde::
      envelope<join_node_request, serde::version<1>, serde::compat_version<0>> {
    join_node_request() noexcept = default;

    explicit join_node_request(
      cluster_version lv,
      cluster_version ev,
      std::vector<uint8_t> nuuid,
      model::broker b)
      : latest_logical_version(lv)
      , node_uuid(nuuid)
      , node(std::move(b))
      , earliest_logical_version(ev) {}

    explicit join_node_request(
      cluster_version lv, cluster_version ev, model::broker b)
      : latest_logical_version(lv)
      , node(std::move(b))
      , earliest_logical_version(ev) {}

    static constexpr int8_t current_version = 1;

    // The highest version that the joining node supports
    cluster_version latest_logical_version{cluster::invalid_version};

    // node_uuid may be empty: this is for future use implementing auto
    // selection of node_id.  Convert to a more convenient type later:
    // the vector is just to reserve the on-disk layout.
    std::vector<uint8_t> node_uuid;
    model::broker node;

    // The lowest version that the joining node supports, it will already
    // have its feature table initialized to this version.
    cluster_version earliest_logical_version{cluster::invalid_version};

    friend bool operator==(const join_node_request&, const join_node_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const join_node_request& r) {
        fmt::print(
          o,
          "logical_version {}-{} node_uuid {} node {}",
          r.earliest_logical_version,
          r.latest_logical_version,
          r.node_uuid,
          r.node);
        return o;
    }

    auto serde_fields() {
        return std::tie(
          latest_logical_version, node_uuid, node, earliest_logical_version);
    }
};

struct join_node_reply
  : serde::
      envelope<join_node_reply, serde::version<0>, serde::compat_version<0>> {
    bool success{false};
    model::node_id id{model::unassigned_node_id};

    join_node_reply() noexcept = default;

    join_node_reply(bool success, model::node_id id)
      : success(success)
      , id(id) {}

    friend bool operator==(const join_node_reply&, const join_node_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const join_node_reply& r) {
        fmt::print(o, "success {} id {}", r.success, r.id);
        return o;
    }

    auto serde_fields() { return std::tie(success, id); }
};

struct configuration_update_request
  : serde::envelope<
      configuration_update_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    configuration_update_request() noexcept = default;
    explicit configuration_update_request(model::broker b, model::node_id tid)
      : node(std::move(b))
      , target_node(tid) {}

    model::broker node;
    model::node_id target_node;

    friend bool operator==(
      const configuration_update_request&, const configuration_update_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const configuration_update_request&);

    auto serde_fields() { return std::tie(node, target_node); }
};

struct configuration_update_reply
  : serde::envelope<
      configuration_update_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    configuration_update_reply() noexcept = default;
    explicit configuration_update_reply(bool success)
      : success(success) {}

    bool success;

    friend bool operator==(
      const configuration_update_reply&, const configuration_update_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const configuration_update_reply&);

    auto serde_fields() { return std::tie(success); }
};

/// Partition assignment describes an assignment of all replicas for single NTP.
/// The replicas are hold in vector of broker_shard.
struct partition_assignment
  : serde::envelope<
      partition_assignment,
      serde::version<0>,
      serde::compat_version<0>> {
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
  : serde::envelope<
      remote_topic_properties,
      serde::version<0>,
      serde::compat_version<0>> {
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

    friend std::ostream&
    operator<<(std::ostream&, const remote_topic_properties&);
};

/**
 * Structure holding topic properties overrides, empty values will be replaced
 * with defaults
 */
struct topic_properties
  : serde::
      envelope<topic_properties, serde::version<4>, serde::compat_version<0>> {
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
      std::optional<remote_topic_properties> remote_topic_properties,
      std::optional<uint32_t> batch_max_bytes,
      tristate<size_t> retention_local_target_bytes,
      tristate<std::chrono::milliseconds> retention_local_target_ms,
      bool remote_delete,
      tristate<std::chrono::milliseconds> segment_ms)
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
      , read_replica_bucket(std::move(read_replica_bucket))
      , remote_topic_properties(remote_topic_properties)
      , batch_max_bytes(batch_max_bytes)
      , retention_local_target_bytes(retention_local_target_bytes)
      , retention_local_target_ms(retention_local_target_ms)
      , remote_delete(remote_delete)
      , segment_ms(segment_ms) {}

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
    std::optional<uint32_t> batch_max_bytes;
    tristate<size_t> retention_local_target_bytes{std::nullopt};
    tristate<std::chrono::milliseconds> retention_local_target_ms{std::nullopt};

    // Remote deletes are enabled by default in new tiered storage topics,
    // disabled by default in legacy topics during upgrade.
    // This is intentionally not an optional: all topics have a concrete value
    // one way or another.  There is no "use the cluster default".
    bool remote_delete{storage::ntp_config::default_remote_delete};

    tristate<std::chrono::milliseconds> segment_ms{std::nullopt};

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
          remote_topic_properties,
          batch_max_bytes,
          retention_local_target_bytes,
          retention_local_target_ms,
          remote_delete,
          segment_ms);
    }

    friend bool operator==(const topic_properties&, const topic_properties&)
      = default;
};

enum incremental_update_operation : int8_t { none, set, remove };

inline std::string_view
incremental_update_operation_as_string(incremental_update_operation op) {
    switch (op) {
    case incremental_update_operation::none:
        return "none";
    case incremental_update_operation::set:
        return "set";
    case incremental_update_operation::remove:
        return "remove";
    default:
        vassert(false, "Unknown operation type passed: {}", int8_t(op));
    }
}

template<typename T>
struct property_update
  : serde::envelope<
      property_update<T>,
      serde::version<0>,
      serde::compat_version<0>> {
    property_update() = default;
    property_update(T v, incremental_update_operation op)
      : value(std::move(v))
      , op(op) {}

    T value;
    incremental_update_operation op = incremental_update_operation::none;

    auto serde_fields() { return std::tie(value, op); }

    friend std::ostream&
    operator<<(std::ostream& o, const property_update<T>& p) {
        fmt::print(
          o,
          "property_update: value: {} op: {}",
          p.value,
          incremental_update_operation_as_string(p.op));
        return o;
    }

    friend bool operator==(const property_update<T>&, const property_update<T>&)
      = default;
};

template<typename T>
struct property_update<tristate<T>>
  : serde::envelope<
      property_update<tristate<T>>,
      serde::version<0>,
      serde::compat_version<0>> {
    property_update()
      : value(std::nullopt){};

    property_update(tristate<T> v, incremental_update_operation op)
      : value(std::move(v))
      , op(op) {}
    tristate<T> value;
    incremental_update_operation op = incremental_update_operation::none;

    auto serde_fields() { return std::tie(value, op); }

    friend std::ostream&
    operator<<(std::ostream& o, const property_update<tristate<T>>& p) {
        fmt::print(
          o,
          "property_update: value: {} op: {}",
          p.value,
          incremental_update_operation_as_string(p.op));
        return o;
    }

    friend bool operator==(
      const property_update<tristate<T>>&, const property_update<tristate<T>>&)
      = default;
};

struct incremental_topic_updates
  : serde::envelope<
      incremental_topic_updates,
      serde::version<3>,
      serde::compat_version<0>> {
    static constexpr int8_t version_with_data_policy = -1;
    static constexpr int8_t version_with_shadow_indexing = -3;
    static constexpr int8_t version_with_batch_max_bytes_and_local_retention
      = -4;
    static constexpr int8_t version_with_segment_ms = -5;
    // negative version indicating different format:
    // -1 - topic_updates with data_policy
    // -2 - topic_updates without data_policy
    // -3 - topic_updates with shadow_indexing
    // -4 - topic update with batch_max_bytes and retention.local.target
    static constexpr int8_t version = version_with_segment_ms;
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
    property_update<std::optional<uint32_t>> batch_max_bytes;
    property_update<tristate<size_t>> retention_local_target_bytes;
    property_update<tristate<std::chrono::milliseconds>>
      retention_local_target_ms;
    property_update<bool> remote_delete{
      false, incremental_update_operation::none};
    property_update<tristate<std::chrono::milliseconds>> segment_ms;

    auto serde_fields() {
        return std::tie(
          compression,
          cleanup_policy_bitflags,
          compaction_strategy,
          timestamp_type,
          segment_size,
          retention_bytes,
          retention_duration,
          shadow_indexing,
          batch_max_bytes,
          retention_local_target_bytes,
          retention_local_target_ms,
          remote_delete,
          segment_ms);
    }

    friend std::ostream&
    operator<<(std::ostream&, const incremental_topic_updates&);

    friend bool operator==(
      const incremental_topic_updates&, const incremental_topic_updates&)
      = default;
};

using replication_factor
  = named_type<uint16_t, struct replication_factor_type_tag>;

std::istream& operator>>(std::istream& i, replication_factor& cs);

replication_factor parsing_replication_factor(const ss::sstring& value);

// This class contains updates for topic properties which are replicates not by
// topic_frontend
struct incremental_topic_custom_updates
  : serde::envelope<
      incremental_topic_custom_updates,
      serde::version<1>,
      serde::compat_version<0>> {
    // Data-policy property is replicated by data_policy_frontend and handled by
    // data_policy_manager.
    property_update<std::optional<v8_engine::data_policy>> data_policy;
    // Replication factor is custom handled.
    property_update<std::optional<replication_factor>> replication_factor;

    friend std::ostream&
    operator<<(std::ostream&, const incremental_topic_custom_updates&);

    friend bool operator==(
      const incremental_topic_custom_updates&,
      const incremental_topic_custom_updates&)
      = default;

    auto serde_fields() { return std::tie(data_policy, replication_factor); }
};

/**
 * Struct representing single topic properties update
 */
struct topic_properties_update
  : serde::envelope<
      topic_properties_update,
      serde::version<0>,
      serde::compat_version<0>> {
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

    friend std::ostream&
    operator<<(std::ostream&, const topic_properties_update&);

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
  : serde::envelope<
      topic_configuration,
      serde::version<1>,
      serde::compat_version<0>> {
    topic_configuration(
      model::ns ns,
      model::topic topic,
      int32_t partition_count,
      int16_t replication_factor)
      : tp_ns(std::move(ns), std::move(topic))
      , partition_count(partition_count)
      , replication_factor(replication_factor) {}

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
    bool is_recovery_enabled() const {
        return properties.recovery && properties.recovery.value();
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

    void serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;

        tp_ns = read_nested<model::topic_namespace>(in, h._bytes_left_limit);
        partition_count = read_nested<int32_t>(in, h._bytes_left_limit);
        replication_factor = read_nested<int16_t>(in, h._bytes_left_limit);
        properties = read_nested<topic_properties>(in, h._bytes_left_limit);

        if (h._version < 1) {
            // Legacy tiered storage topics do not delete data on
            // topic deletion.
            properties.remote_delete
              = storage::ntp_config::legacy_remote_delete;
        }
    }

    static void maybe_adjust_retention_policies(
      std::optional<model::shadow_indexing_mode>,
      tristate<std::size_t>&,
      tristate<std::chrono::milliseconds>&,
      tristate<std::size_t>&,
      tristate<std::chrono::milliseconds>&);

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
    bool is_recovery_enabled() const { return cfg.is_recovery_enabled(); }

    friend std::ostream&
    operator<<(std::ostream&, const custom_assignable_topic_configuration&);
};

struct create_partitions_configuration
  : serde::envelope<
      create_partitions_configuration,
      serde::version<0>,
      serde::compat_version<0>> {
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
  : serde::envelope<
      topic_configuration_assignment,
      serde::version<0>,
      serde::compat_version<0>> {
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
  : serde::envelope<
      create_partitions_configuration_assignment,
      serde::version<0>,
      serde::compat_version<0>> {
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

struct topic_result
  : serde::envelope<topic_result, serde::version<0>, serde::compat_version<0>> {
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

struct create_topics_request
  : serde::envelope<
      create_topics_request,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<topic_configuration> topics;
    model::timeout_clock::duration timeout;

    friend bool
    operator==(const create_topics_request&, const create_topics_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const create_topics_request&);

    auto serde_fields() { return std::tie(topics, timeout); }
};

struct create_topics_reply
  : serde::envelope<
      create_topics_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<topic_result> results;
    std::vector<model::topic_metadata> metadata;
    std::vector<topic_configuration> configs;

    create_topics_reply() noexcept = default;
    create_topics_reply(
      std::vector<topic_result> results,
      std::vector<model::topic_metadata> metadata,
      std::vector<topic_configuration> configs)
      : results(std::move(results))
      , metadata(std::move(metadata))
      , configs(std::move(configs)) {}

    friend bool
    operator==(const create_topics_reply&, const create_topics_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const create_topics_reply&);

    auto serde_fields() { return std::tie(results, metadata, configs); }
};

struct finish_partition_update_request
  : serde::envelope<
      finish_partition_update_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::ntp ntp;
    std::vector<model::broker_shard> new_replica_set;

    friend bool operator==(
      const finish_partition_update_request&,
      const finish_partition_update_request&)
      = default;

    auto serde_fields() { return std::tie(ntp, new_replica_set); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_partition_update_request& r);
};

struct finish_partition_update_reply
  : serde::envelope<
      finish_partition_update_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    cluster::errc result;

    friend bool operator==(
      const finish_partition_update_reply&,
      const finish_partition_update_reply&)
      = default;

    auto serde_fields() { return std::tie(result); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_partition_update_reply& r);
};

struct update_topic_properties_request
  : serde::envelope<
      update_topic_properties_request,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<topic_properties_update> updates;

    friend std::ostream&
    operator<<(std::ostream&, const update_topic_properties_request&);

    friend bool operator==(
      const update_topic_properties_request&,
      const update_topic_properties_request&)
      = default;

    auto serde_fields() { return std::tie(updates); }
};

struct update_topic_properties_reply
  : serde::envelope<
      update_topic_properties_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<topic_result> results;

    friend std::ostream&
    operator<<(std::ostream&, const update_topic_properties_reply&);

    friend bool operator==(
      const update_topic_properties_reply&,

      const update_topic_properties_reply&)
      = default;

    auto serde_fields() { return std::tie(results); }
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

/**
 * Replicas revision map is used to track revision of brokers in a replica
 * set. When a node is added into replica set its gets the revision assigned
 */
using replicas_revision_map
  = absl::flat_hash_map<model::node_id, model::revision_id>;

// delta propagated to backend
struct topic_table_delta {
    enum class op_type {
        add,
        del,
        update,
        update_finished,
        update_properties,
        add_non_replicable,
        del_non_replicable,
        cancel_update,
        force_abort_update,
        reset,
    };

    topic_table_delta(
      model::ntp,
      cluster::partition_assignment,
      model::offset,
      op_type,
      std::optional<std::vector<model::broker_shard>> previous = std::nullopt,
      std::optional<replicas_revision_map> = std::nullopt);

    model::ntp ntp;
    cluster::partition_assignment new_assignment;
    model::offset offset;
    op_type type;
    std::optional<std::vector<model::broker_shard>> previous_replica_set;
    std::optional<replicas_revision_map> replica_revisions;

    model::topic_namespace_view tp_ns() const {
        return model::topic_namespace_view(ntp);
    }

    bool is_reconfiguration_operation() const {
        return type == op_type::update || type == op_type::cancel_update
               || type == op_type::force_abort_update;
    }

    /// Preconditions: delta is of type that has replica_revisions and the node
    /// is in the new assignment.
    model::revision_id get_replica_revision(model::node_id) const;

    friend std::ostream& operator<<(std::ostream&, const topic_table_delta&);
    friend std::ostream& operator<<(std::ostream&, const op_type&);
};

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

struct backend_operation
  : serde::
      envelope<backend_operation, serde::version<0>, serde::compat_version<0>> {
    ss::shard_id source_shard;
    partition_assignment p_as;
    topic_table_delta::op_type type;
    friend std::ostream& operator<<(std::ostream&, const backend_operation&);

    friend bool operator==(const backend_operation&, const backend_operation&)
      = default;

    auto serde_fields() { return std::tie(source_shard, p_as, type); }
};

struct create_data_policy_cmd_data
  : serde::envelope<
      create_data_policy_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1; // In future dp will be vector

    auto serde_fields() { return std::tie(dp); }

    v8_engine::data_policy dp;

    friend bool operator==(
      const create_data_policy_cmd_data&, const create_data_policy_cmd_data&)
      = default;
};

struct non_replicable_topic
  : serde::envelope<
      non_replicable_topic,
      serde::version<0>,
      serde::compat_version<0>> {
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

struct config_status
  : serde::
      envelope<config_status, serde::version<0>, serde::compat_version<0>> {
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
  : serde::envelope<
      cluster_property_kv,
      serde::version<0>,
      serde::compat_version<0>> {
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
    friend std::ostream& operator<<(std::ostream&, const cluster_property_kv&);
};

struct cluster_config_delta_cmd_data
  : serde::envelope<
      cluster_config_delta_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
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
  : serde::envelope<
      cluster_config_status_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    friend bool operator==(
      const cluster_config_status_cmd_data&,
      const cluster_config_status_cmd_data&)
      = default;

    auto serde_fields() { return std::tie(status); }

    config_status status;
};

struct feature_update_action
  : serde::envelope<
      feature_update_action,
      serde::version<0>,
      serde::compat_version<0>> {
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
  : serde::envelope<
      feature_update_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
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

using force_abort_update = ss::bool_class<struct force_abort_update_tag>;

struct cancel_moving_partition_replicas_cmd_data
  : serde::envelope<
      cancel_moving_partition_replicas_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    cancel_moving_partition_replicas_cmd_data() = default;
    explicit cancel_moving_partition_replicas_cmd_data(force_abort_update force)
      : force(force) {}
    force_abort_update force;

    auto serde_fields() { return std::tie(force); }
};

struct move_topic_replicas_data
  : serde::envelope<
      move_topic_replicas_data,
      serde::version<0>,
      serde::compat_version<0>> {
    move_topic_replicas_data() noexcept = default;
    explicit move_topic_replicas_data(
      model::partition_id partition, std::vector<model::broker_shard> replicas)
      : partition(partition)
      , replicas(std::move(replicas)) {}

    model::partition_id partition;
    std::vector<model::broker_shard> replicas;

    auto serde_fields() { return std::tie(partition, replicas); }

    friend std::ostream&
    operator<<(std::ostream&, const move_topic_replicas_data&);
};

struct feature_update_license_update_cmd_data
  : serde::envelope<
      feature_update_license_update_cmd_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    // Struct encoding version
    static constexpr int8_t current_version = 1;

    security::license redpanda_license;

    auto serde_fields() { return std::tie(redpanda_license); }

    friend std::ostream&
    operator<<(std::ostream&, const feature_update_license_update_cmd_data&);
};

struct user_and_credential
  : serde::envelope<
      user_and_credential,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 0;

    user_and_credential() = default;
    user_and_credential(
      security::credential_user&& username_,
      security::scram_credential&& credential_)
      : username(std::move(username_))
      , credential(std::move(credential_)) {}
    friend bool
    operator==(const user_and_credential&, const user_and_credential&)
      = default;
    auto serde_fields() { return std::tie(username, credential); }

    security::credential_user username;
    security::scram_credential credential;
};

struct bootstrap_cluster_cmd_data
  : serde::envelope<
      bootstrap_cluster_cmd_data,
      serde::version<2>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend bool operator==(
      const bootstrap_cluster_cmd_data&, const bootstrap_cluster_cmd_data&)
      = default;

    auto serde_fields() {
        return std::tie(
          uuid,
          bootstrap_user_cred,
          node_ids_by_uuid,
          founding_version,
          initial_nodes);
    }

    model::cluster_uuid uuid;
    std::optional<user_and_credential> bootstrap_user_cred;
    absl::flat_hash_map<model::node_uuid, model::node_id> node_ids_by_uuid;

    // If this is set, fast-forward the feature_table to enable features
    // from this version. Indicates the version of Redpanda of
    // the node that generated the bootstrap record.
    cluster_version founding_version{invalid_version};
    std::vector<model::broker> initial_nodes;
};

enum class reconciliation_status : int8_t {
    done,
    in_progress,
    error,
};
std::ostream& operator<<(std::ostream&, const reconciliation_status&);

class ntp_reconciliation_state
  : public serde::envelope<
      ntp_reconciliation_state,
      serde::version<0>,
      serde::compat_version<0>> {
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
  : serde::envelope<
      reconciliation_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<model::ntp> ntps;

    friend bool operator==(
      const reconciliation_state_request&, const reconciliation_state_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const reconciliation_state_request& req) {
        fmt::print(o, "{{ ntps: {} }}", req.ntps);
        return o;
    }

    auto serde_fields() { return std::tie(ntps); }
};

struct reconciliation_state_reply
  : serde::envelope<
      reconciliation_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<ntp_reconciliation_state> results;

    friend bool operator==(
      const reconciliation_state_reply&, const reconciliation_state_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const reconciliation_state_reply& rep) {
        fmt::print(o, "{{ results {} }}", rep.results);
        return o;
    }

    auto serde_fields() { return std::tie(results); }
};

struct decommission_node_request
  : serde::envelope<
      decommission_node_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id id;

    friend bool operator==(
      const decommission_node_request&, const decommission_node_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const decommission_node_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct decommission_node_reply
  : serde::envelope<
      decommission_node_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;

    friend bool
    operator==(const decommission_node_reply&, const decommission_node_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const decommission_node_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct recommission_node_request
  : serde::envelope<
      recommission_node_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id id;

    friend bool operator==(
      const recommission_node_request&, const recommission_node_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const recommission_node_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct recommission_node_reply
  : serde::envelope<
      recommission_node_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;

    friend bool
    operator==(const recommission_node_reply&, const recommission_node_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const recommission_node_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct finish_reallocation_request
  : serde::envelope<
      finish_reallocation_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id id;

    friend bool operator==(
      const finish_reallocation_request&, const finish_reallocation_request&)
      = default;

    auto serde_fields() { return std::tie(id); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_reallocation_request& r) {
        fmt::print(o, "id {}", r.id);
        return o;
    }
};

struct finish_reallocation_reply
  : serde::envelope<
      finish_reallocation_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    errc error;

    friend bool operator==(
      const finish_reallocation_reply&, const finish_reallocation_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const finish_reallocation_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct set_maintenance_mode_request
  : serde::envelope<
      set_maintenance_mode_request,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    model::node_id id;
    bool enabled;

    friend bool operator==(
      const set_maintenance_mode_request&, const set_maintenance_mode_request&)
      = default;

    auto serde_fields() { return std::tie(id, enabled); }

    friend std::ostream&
    operator<<(std::ostream& o, const set_maintenance_mode_request& r) {
        fmt::print(o, "id {} enabled {}", r.id, r.enabled);
        return o;
    }
};

struct set_maintenance_mode_reply
  : serde::envelope<
      set_maintenance_mode_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    errc error;

    friend bool operator==(
      const set_maintenance_mode_reply&, const set_maintenance_mode_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }

    friend std::ostream&
    operator<<(std::ostream& o, const set_maintenance_mode_reply& r) {
        fmt::print(o, "error {}", r.error);
        return o;
    }
};

struct config_status_request
  : serde::envelope<
      config_status_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    config_status status;

    friend std::ostream&
    operator<<(std::ostream&, const config_status_request&);

    friend bool
    operator==(const config_status_request&, const config_status_request&)
      = default;

    auto serde_fields() { return std::tie(status); }
};

struct config_status_reply
  : serde::envelope<
      config_status_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc error;

    friend std::ostream& operator<<(std::ostream&, const config_status_reply&);

    friend bool
    operator==(const config_status_reply&, const config_status_reply&)
      = default;

    auto serde_fields() { return std::tie(error); }
};

struct feature_action_request
  : serde::envelope<
      feature_action_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    feature_update_action action;

    friend bool
    operator==(const feature_action_request&, const feature_action_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_action_request&);

    auto serde_fields() { return std::tie(action); }
};

struct feature_action_response
  : serde::envelope<
      feature_action_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc error;

    friend bool
    operator==(const feature_action_response&, const feature_action_response&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_action_response&);

    auto serde_fields() { return std::tie(error); }
};

using feature_barrier_tag
  = named_type<ss::sstring, struct feature_barrier_tag_type>;

struct feature_barrier_request
  : serde::envelope<
      feature_barrier_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 1;
    feature_barrier_tag tag; // Each cooperative barrier must use a unique tag
    model::node_id peer;
    bool entered; // Has the requester entered?

    friend bool
    operator==(const feature_barrier_request&, const feature_barrier_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_barrier_request&);

    auto serde_fields() { return std::tie(tag, peer, entered); }
};

struct feature_barrier_response
  : serde::envelope<
      feature_barrier_response,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 1;
    bool entered;  // Has the respondent entered?
    bool complete; // Has the respondent exited?

    friend bool
    operator==(const feature_barrier_response&, const feature_barrier_response&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const feature_barrier_response&);

    auto serde_fields() { return std::tie(entered, complete); }
};

struct create_non_replicable_topics_request
  : serde::envelope<
      create_non_replicable_topics_request,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<non_replicable_topic> topics;
    model::timeout_clock::duration timeout;

    friend bool operator==(
      const create_non_replicable_topics_request&,
      const create_non_replicable_topics_request&)
      = default;

    auto serde_fields() { return std::tie(topics, timeout); }

    friend std::ostream&
    operator<<(std::ostream&, const create_non_replicable_topics_request&);
};

struct create_non_replicable_topics_reply
  : serde::envelope<
      create_non_replicable_topics_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 1;
    std::vector<topic_result> results;

    friend bool operator==(
      const create_non_replicable_topics_reply&,
      const create_non_replicable_topics_reply&)
      = default;

    auto serde_fields() { return std::tie(results); }

    friend std::ostream&
    operator<<(std::ostream&, const create_non_replicable_topics_reply&);
};

struct config_update_request final
  : serde::envelope<
      config_update_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    std::vector<cluster_property_kv> upsert;
    std::vector<ss::sstring> remove;

    friend bool
    operator==(const config_update_request&, const config_update_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const config_update_request&);

    auto serde_fields() { return std::tie(upsert, remove); }
};

struct config_update_reply
  : serde::envelope<
      config_update_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc error;
    cluster::config_version latest_version{config_version_unset};

    friend bool
    operator==(const config_update_reply&, const config_update_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const config_update_reply&);

    auto serde_fields() { return std::tie(error, latest_version); }
};

struct hello_request final
  : serde::
      envelope<hello_request, serde::version<0>, serde::compat_version<0>> {
    model::node_id peer;

    // milliseconds since epoch
    std::chrono::milliseconds start_time;

    friend bool operator==(const hello_request&, const hello_request&)
      = default;

    auto serde_fields() { return std::tie(peer, start_time); }

    friend std::ostream& operator<<(std::ostream&, const hello_request&);
};

struct hello_reply
  : serde::envelope<hello_reply, serde::version<0>, serde::compat_version<0>> {
    errc error;

    friend bool operator==(const hello_reply&, const hello_reply&) = default;

    friend std::ostream& operator<<(std::ostream&, const hello_reply&);

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

struct topic_metadata_fields
  : serde::envelope<
      topic_metadata_fields,
      serde::version<0>,
      serde::compat_version<0>> {
    topic_configuration configuration;
    std::optional<model::topic> source_topic;
    model::revision_id revision;
    std::optional<model::initial_revision_id> remote_revision;

    topic_metadata_fields(
      topic_configuration cfg,
      std::optional<model::topic> st,
      model::revision_id rid,
      std::optional<model::initial_revision_id> remote_revision_id) noexcept;

    // for serde
    topic_metadata_fields() noexcept = default;

    friend bool
    operator==(const topic_metadata_fields&, const topic_metadata_fields&)
      = default;

    auto serde_fields() {
        return std::tie(configuration, source_topic, revision, remote_revision);
    }
};

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

    topic_metadata(topic_metadata_fields, assignments_set) noexcept;

    bool is_topic_replicable() const;
    model::revision_id get_revision() const;
    std::optional<model::initial_revision_id> get_remote_revision() const;
    const model::topic& get_source_topic() const;

    const topic_metadata_fields& get_fields() const { return _fields; }
    topic_metadata_fields& get_fields() { return _fields; }

    const topic_configuration& get_configuration() const;
    topic_configuration& get_configuration();

    const assignments_set& get_assignments() const;
    assignments_set& get_assignments();

    replication_factor get_replication_factor() const;

private:
    topic_metadata_fields _fields;
    assignments_set _assignments;
};

struct move_cancellation_result
  : serde::envelope<
      move_cancellation_result,
      serde::version<0>,
      serde::compat_version<0>> {
    move_cancellation_result() = default;

    move_cancellation_result(model::ntp ntp, cluster::errc ec)
      : ntp(std::move(ntp))
      , result(ec) {}

    auto serde_fields() { return std::tie(ntp, result); }

    friend bool
    operator==(const move_cancellation_result&, const move_cancellation_result&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const move_cancellation_result&);

    model::ntp ntp;
    cluster::errc result;
};

enum class partition_move_direction { to_node, from_node, all };
std::ostream& operator<<(std::ostream&, const partition_move_direction&);

struct cancel_all_partition_movements_request
  : serde::envelope<
      cancel_all_partition_movements_request,
      serde::version<0>,
      serde::compat_version<0>> {
    cancel_all_partition_movements_request() = default;

    auto serde_fields() { return std::tie(); }

    friend bool operator==(
      const cancel_all_partition_movements_request&,
      const cancel_all_partition_movements_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const cancel_all_partition_movements_request&) {
        fmt::print(o, "{{}}");
        return o;
    }
};
struct cancel_node_partition_movements_request
  : serde::envelope<
      cancel_node_partition_movements_request,
      serde::version<0>,
      serde::compat_version<0>> {
    model::node_id node_id;
    partition_move_direction direction;

    auto serde_fields() { return std::tie(node_id, direction); }

    friend bool operator==(
      const cancel_node_partition_movements_request&,
      const cancel_node_partition_movements_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const cancel_node_partition_movements_request&);
};

struct cancel_partition_movements_reply
  : serde::envelope<
      cancel_partition_movements_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    friend bool operator==(
      const cancel_partition_movements_reply&,
      const cancel_partition_movements_reply&)
      = default;

    auto serde_fields() { return std::tie(general_error, partition_results); }

    friend std::ostream&
    operator<<(std::ostream& o, const cancel_partition_movements_reply& r);

    errc general_error;
    std::vector<move_cancellation_result> partition_results;
};

struct cloud_storage_usage_request
  : serde::envelope<
      cloud_storage_usage_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::vector<model::ntp> partitions;

    friend bool operator==(
      const cloud_storage_usage_request&, const cloud_storage_usage_request&)
      = default;

    auto serde_fields() { return std::tie(partitions); }
};

struct cloud_storage_usage_reply
  : serde::envelope<
      cloud_storage_usage_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    uint64_t total_size_bytes{0};

    // When replies are handled in 'cloud_storage_size_reducer'
    // only the size of this list is currently used. However,
    // having the actual missing ntps allws for future optimisations:
    // the request can be retried only for the 'missing_partitions'.
    std::vector<model::ntp> missing_partitions;

    friend bool operator==(
      const cloud_storage_usage_reply&, const cloud_storage_usage_reply&)
      = default;

    auto serde_fields() {
        return std::tie(total_size_bytes, missing_partitions);
    }
};

struct partition_state_request
  : serde::envelope<
      partition_state_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp ntp;
    friend bool
    operator==(const partition_state_request&, const partition_state_request&)
      = default;

    auto serde_fields() { return std::tie(ntp); }
};

struct partition_raft_state
  : serde::envelope<
      partition_raft_state,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id node;
    model::term_id term;
    ss::sstring offset_translator_state;
    ss::sstring group_configuration;
    model::term_id confirmed_term;
    model::offset flushed_offset;
    model::offset commit_index;
    model::offset majority_replicated_index;
    model::offset visibility_upper_bound_index;
    model::offset last_quorum_replicated_index;
    model::term_id last_snapshot_term;
    model::offset last_snapshot_index;
    model::offset received_snapshot_index;
    size_t received_snapshot_bytes;
    bool has_pending_flushes;
    bool is_leader;
    bool is_elected_leader;

    struct follower_state
      : serde::envelope<
          follower_state,
          serde::version<0>,
          serde::compat_version<0>> {
        using rpc_adl_exempt = std::true_type;

        model::node_id node;
        model::offset last_flushed_log_index;
        model::offset last_dirty_log_index;
        model::offset match_index;
        model::offset next_index;
        model::offset last_sent_offset;
        size_t heartbeats_failed;
        bool is_learner;
        uint64_t ms_since_last_heartbeat;
        uint64_t last_sent_seq;
        uint64_t last_received_seq;
        uint64_t last_successful_received_seq;
        bool suppress_heartbeats;
        bool is_recovering;

        auto serde_fields() {
            return std::tie(
              node,
              last_flushed_log_index,
              last_dirty_log_index,
              match_index,
              next_index,
              last_sent_offset,
              heartbeats_failed,
              is_learner,
              ms_since_last_heartbeat,
              last_sent_seq,
              last_received_seq,
              last_successful_received_seq,
              suppress_heartbeats,
              is_recovering);
        }
    };

    // Set only on leaders.
    std::optional<std::vector<follower_state>> followers;

    auto serde_fields() {
        return std::tie(
          node,
          term,
          offset_translator_state,
          group_configuration,
          confirmed_term,
          flushed_offset,
          commit_index,
          majority_replicated_index,
          visibility_upper_bound_index,
          last_quorum_replicated_index,
          last_snapshot_term,
          last_snapshot_index,
          received_snapshot_index,
          received_snapshot_bytes,
          has_pending_flushes,
          is_leader,
          is_elected_leader,
          followers);
    }

    friend bool
    operator==(const partition_raft_state&, const partition_raft_state&)
      = default;
};

struct partition_state
  : serde::
      envelope<partition_state, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::offset start_offset;
    model::offset committed_offset;
    model::offset last_stable_offset;
    model::offset high_water_mark;
    model::offset dirty_offset;
    model::offset latest_configuration_offset;
    model::offset start_cloud_offset;
    model::offset next_cloud_offset;
    model::revision_id revision_id;
    size_t log_size_bytes;
    size_t non_log_disk_size_bytes;
    bool is_read_replica_mode_enabled;
    bool is_remote_fetch_enabled;
    bool is_cloud_data_available;
    ss::sstring read_replica_bucket;
    partition_raft_state raft_state;

    auto serde_fields() {
        return std::tie(
          start_offset,
          committed_offset,
          last_stable_offset,
          high_water_mark,
          dirty_offset,
          latest_configuration_offset,
          start_cloud_offset,
          next_cloud_offset,
          revision_id,
          log_size_bytes,
          non_log_disk_size_bytes,
          is_read_replica_mode_enabled,
          is_remote_fetch_enabled,
          is_cloud_data_available,
          read_replica_bucket,
          raft_state);
    }

    friend bool operator==(const partition_state&, const partition_state&)
      = default;
};

struct partition_state_reply
  : serde::envelope<
      partition_state_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::ntp ntp;
    std::optional<partition_state> state;
    errc error_code;

    friend bool
    operator==(const partition_state_reply&, const partition_state_reply&)
      = default;

    auto serde_fields() { return std::tie(ntp, state, error_code); }
};

struct revert_cancel_partition_move_cmd_data
  : serde::envelope<
      revert_cancel_partition_move_cmd_data,
      serde::version<0>,
      serde::version<0>> {
    model::ntp ntp;

    auto serde_fields() { return std::tie(ntp); }

    friend bool operator==(
      const revert_cancel_partition_move_cmd_data&,
      const revert_cancel_partition_move_cmd_data&)
      = default;
};

struct revert_cancel_partition_move_request
  : serde::envelope<
      revert_cancel_partition_move_request,
      serde::version<0>,
      serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::ntp ntp;

    auto serde_fields() { return std::tie(ntp); }

    friend bool operator==(
      const revert_cancel_partition_move_request&,
      const revert_cancel_partition_move_request&)
      = default;
};

struct revert_cancel_partition_move_reply
  : serde::envelope<
      revert_cancel_partition_move_reply,
      serde::version<0>,
      serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    errc result;

    auto serde_fields() { return std::tie(result); }

    friend bool operator==(
      const revert_cancel_partition_move_reply&,
      const revert_cancel_partition_move_reply&)
      = default;
};

/**
 * Broker state transitions are coordinated centrally as opposite to
 * configuration which change is requested by the described node itself. Broker
 * state represents centrally managed node properties. The difference between
 * broker state and configuration is that the configuration change is made on
 * the node while state changes are managed by the cluster controller.
 */
class broker_state
  : public serde::
      envelope<broker_state, serde::version<0>, serde::compat_version<0>> {
public:
    model::membership_state get_membership_state() const {
        return _membership_state;
    }
    void set_membership_state(model::membership_state st) {
        _membership_state = st;
    }

    model::maintenance_state get_maintenance_state() const {
        return _maintenance_state;
    }
    void set_maintenance_state(model::maintenance_state st) {
        _maintenance_state = st;
    }
    friend bool operator==(const broker_state&, const broker_state&) = default;

    friend std::ostream& operator<<(std::ostream&, const broker_state&);

    auto serde_fields() {
        return std::tie(_membership_state, _maintenance_state);
    }

private:
    model::membership_state _membership_state = model::membership_state::active;
    model::maintenance_state _maintenance_state
      = model::maintenance_state::inactive;
};

/**
 * Node metadata describes a cluster node with its state and configuration
 */
struct node_metadata {
    model::broker broker;
    broker_state state;

    friend bool operator==(const node_metadata&, const node_metadata&)
      = default;
    friend std::ostream& operator<<(std::ostream&, const node_metadata&);
};

// Node update types, used for communication between members_manager and
// members_backend.
//
// NOTE: maintenance mode doesn't interact with the members_backend,
// instead interacting with each core via their respective drain_manager.
enum class node_update_type : int8_t {
    // A node has been added to the cluster.
    added,

    // A node has been decommissioned from the cluster.
    decommissioned,

    // A node has been recommissioned after an incomplete decommission.
    recommissioned,

    // All reallocations associated with a given node update have completed
    // (e.g. it's been fully decommissioned, indicating it can no longer be
    // recommissioned).
    reallocation_finished,

    // node has been removed from the cluster
    removed,

    // previous updates must be interrupted
    interrupted,
};

std::ostream& operator<<(std::ostream&, const node_update_type&);

/**
 * Reconfiguration state indicates if ongoing reconfiguration is a result of
 * partition movement, cancellation or forced cancellation
 */
enum class reconfiguration_state { in_progress, cancelled, force_cancelled };

std::ostream& operator<<(std::ostream&, reconfiguration_state);

struct replica_bytes {
    model::node_id node;
    size_t bytes{0};
};

struct partition_reconfiguration_state {
    model::ntp ntp;
    // assignments
    std::vector<model::broker_shard> previous_assignment;
    std::vector<model::broker_shard> current_assignment;
    // state indicating if reconfiguration was cancelled or requested
    reconfiguration_state state;
    // amount of bytes already transferred to new replicas
    std::vector<replica_bytes> already_transferred_bytes;
    // current size of partition
    size_t current_partition_size{0};
};

struct node_decommission_progress {
    // indicate if node decommissioning finished
    bool finished = false;
    // number of replicas left on decommissioned node
    size_t replicas_left{0};
    // list of currently ongoing partition reconfigurations
    std::vector<partition_reconfiguration_state> current_reconfigurations;
};

/*
 * Partition Allocation Domains is the way to make certain partition replicas
 * distributed evenly across the nodes of the cluster. When partition allocation
 * is done within any domain but `common`, all existing allocations outside
 * of that domain will be ignored while assigning partition a node.
 * The `common` domain will consider allocations in all domains.
 * Negative values are used for hardcoded domains, positive values are reserved
 * for future use as user assigned domains, and may be used for a feature that
 * would allow users to designate certain topics to have their partition
 * replicas and leaders evenly distrbuted regardless of other topics.
 */
using partition_allocation_domain
  = named_type<int32_t, struct partition_allocation_domain_tag>;
namespace partition_allocation_domains {
constexpr auto common = partition_allocation_domain(0);
constexpr auto consumer_offsets = partition_allocation_domain(-1);
} // namespace partition_allocation_domains

enum class cloud_storage_mode : uint8_t {
    disabled = 0,
    write_only = 1,
    read_only = 2,
    full = 3,
    read_replica = 4
};

std::ostream& operator<<(std::ostream&, const cloud_storage_mode&);

struct partition_cloud_storage_status {
    cloud_storage_mode mode;

    std::optional<std::chrono::milliseconds> since_last_manifest_upload;
    std::optional<std::chrono::milliseconds> since_last_segment_upload;
    std::optional<std::chrono::milliseconds> since_last_manifest_sync;

    size_t total_log_size_bytes{0};
    size_t cloud_log_size_bytes{0};
    size_t local_log_size_bytes{0};

    size_t cloud_log_segment_count{0};
    size_t local_log_segment_count{0};

    // Friendlier name for archival_metadata_stm::get_dirty
    bool cloud_metadata_update_pending{false};

    std::optional<kafka::offset> cloud_log_start_offset;
    std::optional<kafka::offset> local_log_last_offset;

    std::optional<kafka::offset> cloud_log_last_offset;
    std::optional<kafka::offset> local_log_start_offset;
};

struct metrics_reporter_cluster_info
  : serde::envelope<
      metrics_reporter_cluster_info,
      serde::version<0>,
      serde::compat_version<0>> {
    ss::sstring uuid;
    model::timestamp creation_timestamp;

    bool is_initialized() const { return !uuid.empty(); }

    friend bool operator==(
      const metrics_reporter_cluster_info&,
      const metrics_reporter_cluster_info&)
      = default;

    auto serde_fields() { return std::tie(uuid, creation_timestamp); }
};

} // namespace cluster
namespace std {
template<>
struct is_error_code_enum<cluster::tx_errc> : true_type {};
} // namespace std

namespace reflection {

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
          r.timeout,
          r.tm_partition);
    }
    cluster::begin_group_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto group_id = adl<kafka::group_id>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        auto tm_partition = adl<model::partition_id>{}.from(in);
        return {
          std::move(ntp),
          std::move(group_id),
          pid,
          tx_seq,
          timeout,
          tm_partition};
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
          out,
          std::move(r.ntp),
          r.pid,
          r.tx_seq,
          r.transaction_timeout_ms,
          r.tm_partition);
    }
    cluster::begin_tx_request from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto pid = adl<model::producer_identity>{}.from(in);
        auto tx_seq = adl<model::tx_seq>{}.from(in);
        auto timeout = adl<std::chrono::milliseconds>{}.from(in);
        auto tm_partition = adl<model::partition_id>{}.from(in);
        return {std::move(ntp), pid, tx_seq, timeout, tm_partition};
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

template<>
struct adl<cluster::delete_acls_request> {
    void to(iobuf& out, cluster::delete_acls_request&& r) {
        serialize(out, std::move(r.data), r.timeout);
    }
    cluster::delete_acls_request from(iobuf_parser& in) {
        auto data = adl<cluster::delete_acls_cmd_data>{}.from(in);
        auto timeout = adl<model::timeout_clock::duration>{}.from(in);
        return {std::move(data), timeout};
    }
};

template<>
struct adl<cluster::delete_acls_reply> {
    void to(iobuf& out, cluster::delete_acls_reply&& r) {
        serialize(out, std::move(r.results));
    }
    cluster::delete_acls_reply from(iobuf_parser& in) {
        auto results = adl<std::vector<cluster::delete_acls_result>>{}.from(in);
        return {.results = std::move(results)};
    }
};

template<>
struct adl<cluster::decommission_node_request> {
    void to(iobuf& out, cluster::decommission_node_request&& r) {
        serialize(out, r.id);
    }
    cluster::decommission_node_request from(iobuf_parser& in) {
        auto id = adl<model::node_id>{}.from(in);
        return {.id = id};
    }
};

template<>
struct adl<cluster::decommission_node_reply> {
    void to(iobuf& out, cluster::decommission_node_reply&& r) {
        serialize(out, r.error);
    }
    cluster::decommission_node_reply from(iobuf_parser& in) {
        auto error = adl<cluster::errc>{}.from(in);
        return {.error = error};
    }
};

template<>
struct adl<cluster::recommission_node_request> {
    void to(iobuf& out, cluster::recommission_node_request&& r) {
        serialize(out, r.id);
    }
    cluster::recommission_node_request from(iobuf_parser& in) {
        auto id = adl<model::node_id>{}.from(in);
        return {.id = id};
    }
};

template<>
struct adl<cluster::recommission_node_reply> {
    void to(iobuf& out, cluster::recommission_node_reply&& r) {
        serialize(out, r.error);
    }
    cluster::recommission_node_reply from(iobuf_parser& in) {
        auto error = adl<cluster::errc>{}.from(in);
        return {.error = error};
    }
};

template<>
struct adl<cluster::finish_reallocation_request> {
    void to(iobuf& out, cluster::finish_reallocation_request&& r) {
        serialize(out, r.id);
    }
    cluster::finish_reallocation_request from(iobuf_parser& in) {
        auto id = adl<model::node_id>{}.from(in);
        return {.id = id};
    }
};

template<>
struct adl<cluster::finish_reallocation_reply> {
    void to(iobuf& out, cluster::finish_reallocation_reply&& r) {
        serialize(out, r.error);
    }
    cluster::finish_reallocation_reply from(iobuf_parser& in) {
        auto error = adl<cluster::errc>{}.from(in);
        return {.error = error};
    }
};

template<>
struct adl<cluster::cancel_moving_partition_replicas_cmd_data> {
    void
    to(iobuf& out, cluster::cancel_moving_partition_replicas_cmd_data&& d) {
        serialize(out, d.force);
    }
    cluster::cancel_moving_partition_replicas_cmd_data from(iobuf_parser& in) {
        auto force = adl<cluster::force_abort_update>{}.from(in);
        return cluster::cancel_moving_partition_replicas_cmd_data(force);
    }
};

template<>
struct adl<cluster::move_cancellation_result> {
    void to(iobuf& out, cluster::move_cancellation_result&& r) {
        serialize(out, std::move(r.ntp), r.result);
    }
    cluster::move_cancellation_result from(iobuf_parser& in) {
        auto ntp = adl<model::ntp>{}.from(in);
        auto ec = adl<cluster::errc>{}.from(in);
        return {std::move(ntp), ec};
    }
};

template<>
struct adl<cluster::cancel_node_partition_movements_request> {
    void to(iobuf& out, cluster::cancel_node_partition_movements_request&& r) {
        serialize(out, r.node_id, r.direction);
    }

    cluster::cancel_node_partition_movements_request from(iobuf_parser& in) {
        auto node_id = adl<model::node_id>{}.from(in);
        auto dir = adl<cluster::partition_move_direction>{}.from(in);
        return cluster::cancel_node_partition_movements_request{
          .node_id = node_id, .direction = dir};
    }
};

template<>
struct adl<cluster::cancel_all_partition_movements_request> {
    void to(iobuf&, cluster::cancel_all_partition_movements_request&&) {}

    cluster::cancel_all_partition_movements_request from(iobuf_parser&) {
        return cluster::cancel_all_partition_movements_request{};
    }
};

template<>
struct adl<cluster::cancel_partition_movements_reply> {
    void to(iobuf& out, cluster::cancel_partition_movements_reply&& r) {
        serialize(out, r.general_error, std::move(r.partition_results));
    }

    cluster::cancel_partition_movements_reply from(iobuf_parser& in) {
        auto general_error = adl<cluster::errc>{}.from(in);
        auto partition_results
          = adl<std::vector<cluster::move_cancellation_result>>{}.from(in);

        return cluster::cancel_partition_movements_reply{
          .general_error = general_error,
          .partition_results = std::move(partition_results),
        };
    }
};

} // namespace reflection

namespace absl {

template<typename K, typename V>
std::ostream& operator<<(std::ostream& o, const absl::flat_hash_map<K, V>& r) {
    o << "{";
    bool first = true;
    for (const auto& [k, v] : r) {
        if (!first) {
            o << ", ";
        }
        o << "{" << k << " -> " << v << "}";
        first = false;
    }
    o << "}";
    return o;
}

} // namespace absl
