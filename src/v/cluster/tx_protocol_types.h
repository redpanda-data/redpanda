/**
 * Copyright 2024 Redpanda Data, Inc.
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
#include "cluster/tx_errc.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "serde/rw/bool_class.h"
#include "serde/rw/chrono.h"
#include "serde/rw/enum.h"
#include "serde/rw/vector.h"

namespace cluster {

struct try_abort_reply
  : serde::
      envelope<try_abort_reply, serde::version<0>, serde::compat_version<0>> {
    using committed_type = ss::bool_class<struct committed_type_tag>;
    using aborted_type = ss::bool_class<struct aborted_type_tag>;

    using rpc_adl_exempt = std::true_type;

    committed_type commited;
    aborted_type aborted;
    tx::errc ec;

    try_abort_reply() noexcept = default;

    try_abort_reply(committed_type committed, aborted_type aborted, tx::errc ec)
      : commited(committed)
      , aborted(aborted)
      , ec(ec) {}

    explicit try_abort_reply(tx::errc ec)
      : ec(ec) {}

    friend bool operator==(const try_abort_reply&, const try_abort_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const try_abort_reply& r);

    static try_abort_reply make_aborted() {
        return {committed_type::no, aborted_type::yes, tx::errc::none};
    }

    static try_abort_reply make_committed() {
        return {committed_type::yes, aborted_type::no, tx::errc::none};
    }

    auto serde_fields() { return std::tie(commited, aborted, ec); }
};

struct try_abort_request
  : serde::
      envelope<try_abort_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using reply = try_abort_reply;
    static constexpr const std::string_view name = "try_abort";

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

struct init_tm_tx_request
  : serde::envelope<
      init_tm_tx_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;

    // partition_not_exists, not_leader, topic_not_exists
    model::producer_identity pid;
    tx::errc ec;

    init_tm_tx_reply() noexcept = default;

    init_tm_tx_reply(model::producer_identity pid, tx::errc ec)
      : pid(pid)
      , ec(ec) {}

    friend bool operator==(const init_tm_tx_reply&, const init_tm_tx_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const init_tm_tx_reply& r);

    explicit init_tm_tx_reply(tx::errc ec)
      : ec(ec) {}

    auto serde_fields() { return std::tie(pid, ec); }
};

struct add_partitions_tx_request {
    struct topic {
        model::topic name{};
        std::vector<model::partition_id> partitions{};
        friend std::ostream& operator<<(std::ostream&, const topic&);
    };
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    std::vector<topic> topics{};
};
struct add_partitions_tx_reply {
    struct partition_result {
        model::partition_id partition_index{};
        tx::errc error_code{};
    };
    struct topic_result {
        model::topic name{};
        std::vector<add_partitions_tx_reply::partition_result> results{};
    };
    std::vector<add_partitions_tx_reply::topic_result> results{};
};
struct add_offsets_tx_request {
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    kafka::group_id group_id{};
};
struct add_offsets_tx_reply {
    tx::errc error_code{};
};
struct end_tx_request {
    kafka::transactional_id transactional_id{};
    kafka::producer_id producer_id{};
    int16_t producer_epoch{};
    bool committed{};
};
struct end_tx_reply {
    tx::errc error_code{};
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

        friend std::ostream& operator<<(std::ostream& o, const tx_group& r);

        auto serde_fields() { return std::tie(group_id, etag); }
    };

    tx::errc ec{};
    model::producer_identity pid{};
    model::producer_identity last_pid{};
    model::tx_seq tx_seq{};
    std::chrono::milliseconds timeout_ms{};
    tx_status status{};
    std::vector<tx_partition> partitions{};
    std::vector<tx_group> groups{};

    fetch_tx_reply() noexcept = default;

    fetch_tx_reply(tx::errc ec)
      : ec(ec) {}

    fetch_tx_reply(
      tx::errc ec,
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
    using rpc_adl_exempt = std::true_type;
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
    using rpc_adl_exempt = std::true_type;
    model::ntp ntp;
    model::term_id etag;
    tx::errc ec;
    model::revision_id topic_revision = model::revision_id(-1);

    begin_tx_reply() noexcept = default;

    begin_tx_reply(
      model::ntp ntp,
      model::term_id etag,
      tx::errc ec,
      model::revision_id topic_revision)
      : ntp(std::move(ntp))
      , etag(etag)
      , ec(ec)
      , topic_revision(topic_revision) {}

    begin_tx_reply(model::ntp ntp, model::term_id etag, tx::errc ec)
      : ntp(std::move(ntp))
      , etag(etag)
      , ec(ec) {}

    begin_tx_reply(model::ntp ntp, tx::errc ec)
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
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;

    tx::errc ec{};

    prepare_tx_reply() noexcept = default;

    explicit prepare_tx_reply(tx::errc ec)
      : ec(ec) {}

    friend bool operator==(const prepare_tx_reply&, const prepare_tx_reply&)
      = default;

    friend std::ostream& operator<<(std::ostream& o, const prepare_tx_reply& r);

    auto serde_fields() { return std::tie(ec); }
};

struct commit_tx_request
  : serde::
      envelope<commit_tx_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;

    tx::errc ec{};

    commit_tx_reply() noexcept = default;

    explicit commit_tx_reply(tx::errc ec)
      : ec(ec) {}

    friend bool operator==(const commit_tx_reply&, const commit_tx_reply&)
      = default;

    auto serde_fields() { return std::tie(ec); }

    friend std::ostream& operator<<(std::ostream& o, const commit_tx_reply& r);
};

struct abort_tx_request
  : serde::
      envelope<abort_tx_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;

    tx::errc ec{};

    abort_tx_reply() noexcept = default;

    explicit abort_tx_reply(tx::errc ec)
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
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;

    model::term_id etag;
    tx::errc ec{};

    begin_group_tx_reply() noexcept = default;

    explicit begin_group_tx_reply(tx::errc ec)
      : ec(ec) {}

    begin_group_tx_reply(model::term_id etag, tx::errc ec)
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
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;

    tx::errc ec{};

    prepare_group_tx_reply() noexcept = default;

    explicit prepare_group_tx_reply(tx::errc ec)
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
    using rpc_adl_exempt = std::true_type;

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
    using rpc_adl_exempt = std::true_type;
    tx::errc ec{};

    commit_group_tx_reply() noexcept = default;

    explicit commit_group_tx_reply(tx::errc ec)
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
    using rpc_adl_exempt = std::true_type;
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
    using rpc_adl_exempt = std::true_type;
    tx::errc ec{};

    abort_group_tx_reply() noexcept = default;

    explicit abort_group_tx_reply(tx::errc ec)
      : ec(ec) {}

    friend bool
    operator==(const abort_group_tx_reply&, const abort_group_tx_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const abort_group_tx_reply& r);

    auto serde_fields() { return std::tie(ec); }
};

struct find_coordinator_reply
  : serde::envelope<
      find_coordinator_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::optional<model::node_id> coordinator{std::nullopt};
    std::optional<model::ntp> ntp{std::nullopt};
    errc ec{errc::generic_tx_error}; // this should have been tx::errc

    find_coordinator_reply() noexcept = default;

    // this is a hack to blend find_coordinator_reply into do_route_locally
    find_coordinator_reply(tx::errc tx_ec)
      : coordinator(std::nullopt)
      , ntp(std::nullopt) {
        if (tx_ec == tx::errc::none) {
            ec = errc::success;
        } else if (tx_ec == tx::errc::shard_not_found) {
            ec = errc::not_leader;
        } else {
            ec = errc::generic_tx_error;
        }
    }

    find_coordinator_reply(errc ec)
      : coordinator(std::nullopt)
      , ntp(std::nullopt)
      , ec(ec) {}

    find_coordinator_reply(
      std::optional<model::node_id> coordinator,
      std::optional<model::ntp> ntp,
      errc ec)
      : coordinator(coordinator)
      , ntp(ntp)
      , ec(ec) {}

    friend bool
    operator==(const find_coordinator_reply&, const find_coordinator_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const find_coordinator_reply& r);

    auto serde_fields() { return std::tie(coordinator, ntp, ec); }
};

struct find_coordinator_request
  : serde::envelope<
      find_coordinator_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    using reply = find_coordinator_reply;
    static constexpr const std::string_view name = "find_coordinator";

    kafka::transactional_id tid;

    find_coordinator_request() noexcept = default;

    find_coordinator_request(kafka::transactional_id tid)
      : tid(std::move(tid)) {}

    friend bool
    operator==(const find_coordinator_request&, const find_coordinator_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const find_coordinator_request& r);

    auto serde_fields() { return std::tie(tid); }
};
} // namespace cluster
