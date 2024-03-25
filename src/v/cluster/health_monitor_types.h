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
#include "cluster/drain_manager.h"
#include "cluster/errc.h"
#include "cluster/node/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "utils/delta_for.h"
#include "utils/named_type.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/util/bool_class.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace cluster {

static constexpr ss::shard_id health_monitor_backend_shard = 0;
/**
 * Health reports
 */

using alive = ss::bool_class<struct node_alive_tag>;

// An application version is a software release, like v1.2.3_gfa0d09f8a
using application_version = named_type<ss::sstring, struct version_number_tag>;

/**
 * node state is determined from controller, and it doesn't require contacting
 * with the node directly
 */
struct node_state
  : serde::envelope<node_state, serde::version<0>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;
    node_state(
      model::node_id id,
      model::membership_state membership_state,
      alive is_alive);

    node_state() = default;
    node_state(const node_state&) = default;
    node_state(node_state&&) noexcept = default;
    node_state& operator=(const node_state&) = default;
    node_state& operator=(node_state&&) noexcept = default;
    ~node_state() noexcept = default;

    model::node_id id() const { return _id; }

    model::membership_state membership_state() const {
        return _membership_state;
    }
    // clang-format off
    [[deprecated("please use health_monitor_frontend::is_alive() to query for "
                 "liveness")]] 
    alive is_alive() const {
        return _is_alive;
    }
    // clang-format on
    friend std::ostream& operator<<(std::ostream&, const node_state&);

    friend bool operator==(const node_state&, const node_state&) = default;

    auto serde_fields() { return std::tie(_id, _membership_state, _is_alive); }

private:
    model::node_id _id;
    model::membership_state _membership_state;
    alive _is_alive;
};

struct partition_status
  : serde::
      envelope<partition_status, serde::version<3>, serde::compat_version<0>> {
    static constexpr size_t invalid_size_bytes = size_t(-1);
    static constexpr uint32_t invalid_shard_id = uint32_t(-1);

    model::partition_id id;
    model::term_id term;
    std::optional<model::node_id> leader_id;
    model::revision_id revision_id;
    size_t size_bytes;
    std::optional<uint8_t> under_replicated_replicas;

    /*
     * estimated amount of data subject to reclaim under disk pressure without
     * violating safety guarantees. this is useful for the partition balancer
     * which is interested in free space on a node. a node may have very little
     * physical free space, but have effective free space represented by
     * reclaimable size bytes.
     *
     * an intuitive relationship between size_bytes and reclaimable_size_bytes
     * would have the former being >= than the later. however due to the way
     * that data is collected it is conceivable that this inequality doesn't
     * hold. callers should check for this condition and normalize the values or
     * ignore the update.
     */
    std::optional<size_t> reclaimable_size_bytes;

    uint32_t shard = invalid_shard_id;

    auto serde_fields() {
        return std::tie(
          id,
          term,
          leader_id,
          revision_id,
          size_bytes,
          under_replicated_replicas,
          reclaimable_size_bytes,
          shard);
    }

    friend std::ostream& operator<<(std::ostream&, const partition_status&);
    friend bool operator==(const partition_status&, const partition_status&)
      = default;
};

using partition_statuses_t = chunked_vector<partition_status>;

struct topic_status
  : serde::envelope<topic_status, serde::version<0>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    topic_status() = default;
    topic_status(model::topic_namespace, partition_statuses_t);
    topic_status& operator=(const topic_status&);
    topic_status(const topic_status&);
    topic_status& operator=(topic_status&&) = default;
    topic_status(topic_status&&) = default;
    ~topic_status() = default;

    model::topic_namespace tp_ns;
    partition_statuses_t partitions;
    friend std::ostream& operator<<(std::ostream&, const topic_status&);
    friend bool operator==(const topic_status&, const topic_status&);

    auto serde_fields() { return std::tie(tp_ns, partitions); }
};

/**
 * Node health report is collected built based on node local state at given
 * instance of time
 */
struct node_health_report
  : serde::envelope<
      node_health_report,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 2;

    node_health_report() = default;

    node_health_report(
      model::node_id,
      node::local_state,
      chunked_vector<topic_status>,
      bool include_drain_status,
      std::optional<drain_manager::drain_status>);

    node_health_report(const node_health_report&);
    node_health_report& operator=(const node_health_report&);

    node_health_report(node_health_report&&) = default;
    node_health_report& operator=(node_health_report&&) = default;
    ~node_health_report() = default;

    model::node_id id;
    node::local_state local_state;
    chunked_vector<topic_status> topics;

    /*
     * nodes running old versions of redpanda will assert that they can decode
     * a message they receive by requiring the encoded version to be <= to the
     * latest that that node understands.
     *
     * when drain_status is added the version is bumped, which means that older
     * nodes will crash if they try to decode such a message. this is common for
     * many places in the code base, but node_health_report makes this problem
     * particularly acute because nodes are polled automatically at a regular,
     * short interval.
     *
     * one solution is to make the feature table available in free functions so
     * that we can use it to query about maintenance mode cluster support in
     * adl<T>. unfortunately that won't work well in our mult-node unit tests
     * because thread_local references to the feature service will be clobbered.
     *
     * another option would be to add a constructor to node_health_report so
     * that when a report was created we could record a `serialized_as` version
     * and query the feature table at the call site. this doesn't work well
     * because reflection/adl needs types to be default-constructable.
     *
     * the final solution, which isn't a panacea, is do the equivalent of the
     * ctor trick described above but with a flag. it's not a universal solution
     * because devs need to be aware and handle this manually. fortunately there
     * is only one or two places where we create this object.
     */
    bool include_drain_status{false}; // not serialized
    std::optional<drain_manager::drain_status> drain_status;

    auto serde_fields() {
        return std::tie(id, local_state, topics, drain_status);
    }

    friend std::ostream& operator<<(std::ostream&, const node_health_report&);

    friend bool
    operator==(const node_health_report& a, const node_health_report& b);
};

/**
 * Frame size used in partition_status_cstore
 */
static constexpr size_t frame_size = 2048;

/**
 * Values in partition status columns are not sorted hence we need to use XOR
 * based encoding
 */
template<typename T>
using unsorted_col_t = deltafor_column<T, ::details::delta_xor, frame_size>;

/**
 * Tuple of partition_cstore iterators.
 */
using partition_cstore_iterators_t = std::tuple<
  unsorted_col_t<int32_t>::lw_const_iterator,
  unsorted_col_t<int64_t>::lw_const_iterator,
  unsorted_col_t<int64_t>::lw_const_iterator,
  unsorted_col_t<int64_t>::lw_const_iterator,
  unsorted_col_t<uint64_t>::lw_const_iterator,
  unsorted_col_t<uint8_t>::lw_const_iterator,
  unsorted_col_t<uint64_t>::lw_const_iterator,
  unsorted_col_t<uint32_t>::lw_const_iterator>;

/**
 * An iterator materializing columnar data in partition status.
 * Materialization is lazy and only done when iterator is dereferenced.
 */
class partition_status_materializing_iterator
  : public boost::iterator_facade<
      partition_status_materializing_iterator,
      const partition_status,
      boost::iterators::forward_traversal_tag> {
public:
    partition_status_materializing_iterator() = default;

    partition_status_materializing_iterator(
      const partition_status_materializing_iterator& other)
      = default;

    partition_status_materializing_iterator&
    operator=(const partition_status_materializing_iterator& other)
      = default;

    partition_status_materializing_iterator(
      partition_status_materializing_iterator&&) noexcept
      = default;

    partition_status_materializing_iterator&
    operator=(partition_status_materializing_iterator&&) noexcept
      = default;

    explicit partition_status_materializing_iterator(
      partition_cstore_iterators_t);

    ~partition_status_materializing_iterator() = default;

    size_t index() const { return std::get<0>(_iterators).index(); }

    bool is_end() const { return std::get<0>(_iterators).is_end(); }

private:
    friend class boost::iterator_core_access;

    const partition_status& dereference() const;
    void increment();
    bool equal(const partition_status_materializing_iterator& other) const;

    template<size_t idx, typename T>
    T get_as() const {
        return T(*std::get<idx>(_iterators));
    }

    partition_cstore_iterators_t _iterators;
    mutable std::optional<partition_status> _current;
};
/**
 * Columnar store of partition statues, the store compresses all values in a
 * form of delta_for columns. Each column is responsible for storing values of
 * a single field fo partition_status structure. Partition status can be
 * retrieved by using the iterator which materializes the `partition_status`when
 * dereferenced.
 */
class partition_statuses_cstore
  : public serde::envelope<
      partition_statuses_cstore,
      serde::version<0>,
      serde::compat_version<0>> {
private:
    enum column_idx {
        id,
        term,
        leader,
        revision_id,
        size_bytes,
        under_replicated_replicas,
        reclaimable_size_bytes,
        shard,
    };
    auto columns() const {
        return std::tie(
          _id,
          _term,
          _leader,
          _revision_id,
          _size_bytes,
          _under_replicated_replicas,
          _reclaimable_size_bytes,
          _shard);
    }

    auto columns() {
        return std::tie(
          _id,
          _term,
          _leader,
          _revision_id,
          _size_bytes,
          _under_replicated_replicas,
          _reclaimable_size_bytes,
          _shard);
    }

public:
    partition_statuses_cstore() = default;

    partition_statuses_cstore(partition_statuses_cstore&&) noexcept = default;
    partition_statuses_cstore& operator=(partition_statuses_cstore&&) noexcept
      = default;
    partition_statuses_cstore& operator=(const partition_statuses_cstore&)
      = delete;
    partition_statuses_cstore(const partition_statuses_cstore&) = delete;
    ~partition_statuses_cstore() = default;

    auto serde_fields() {
        return std::tie(
          _id,
          _term,
          _leader,
          _revision_id,
          _size_bytes,
          _under_replicated_replicas,
          _reclaimable_size_bytes,
          _shard,
          _size);
    }

    partition_status_materializing_iterator begin() const;
    partition_status_materializing_iterator end() const;

    /**
     * Return number of stored partition statues
     */
    size_t size() const { return _size; }

    bool empty() const { return _size == 0; }

    /**
     * Appends partition status to the container.
     */
    void push_back(const partition_status&);

    partition_statuses_cstore copy() const;

    friend bool operator==(
      const partition_statuses_cstore&, const partition_statuses_cstore&)
      = default;

private:
    friend partition_status_materializing_iterator;

    partition_cstore_iterators_t internal_begin() const {
        return std::apply(
          [](auto&&... col) {
              return partition_cstore_iterators_t(col.lw_begin()...);
          },
          columns());
    }

    partition_cstore_iterators_t internal_end() const {
        return std::apply(
          [](auto&&... col) {
              return partition_cstore_iterators_t(col.lw_end()...);
          },
          columns());
    }

    unsorted_col_t<int32_t> _id;
    unsorted_col_t<int64_t> _term;
    unsorted_col_t<int64_t> _leader;
    unsorted_col_t<int64_t> _revision_id;
    unsorted_col_t<uint64_t> _size_bytes;
    unsorted_col_t<uint8_t> _under_replicated_replicas;
    unsorted_col_t<uint64_t> _reclaimable_size_bytes;
    unsorted_col_t<uint32_t> _shard;
    size_t _size{0};
};

/**
 * Class representing a range in partition_statues_cstore.
 *
 * The range is used to represent a partition statues that belongs to particular
 * topic.
 */
class partition_statuses_cstore_range {
public:
    partition_statuses_cstore_range(
      size_t size,
      partition_status_materializing_iterator begin,
      partition_status_materializing_iterator end)
      : _size(size)
      , _begin(std::move(begin))
      , _end(std::move(end)) {}

    const partition_status_materializing_iterator& begin() const {
        return _begin;
    }

    const partition_status_materializing_iterator& end() const { return _end; }

    size_t size() const { return _size; }

private:
    size_t _size;
    partition_status_materializing_iterator _begin;
    partition_status_materializing_iterator _end;
};
namespace details {
/**
 * Class used to represent topic and position of its partition statues in
 * partition status columnar store.
 */
struct topic_range
  : serde::envelope<topic_range, serde::version<0>, serde::compat_version<0>> {
    topic_range() = default;

    topic_range(model::topic tp, size_t ns_idx, size_t partition_count);

    model::topic tp;
    size_t namespace_index{0};
    size_t partition_count{0};

    auto serde_fields() {
        return std::tie(tp, namespace_index, partition_count);
    }

    friend bool operator==(const topic_range&, const topic_range&) = default;
};

} // namespace details

/**
 * Simple helper struct that is materialized when topic state iterator is
 * dereferenced. The topic state iterator is an iterator facade that allows to
 * iterate over individual topics and their partition statues.
 */
struct topic_status_view {
    topic_status_view(
      const model::ns&,
      const model::topic&,
      size_t partition_count,
      partition_status_materializing_iterator begin,
      partition_status_materializing_iterator end);

    model::topic_namespace_view tp_ns;
    partition_statuses_cstore_range partitions;

    friend std::ostream& operator<<(std::ostream&, const topic_status_view&);
};

class topics_store;
using topic_ranges_t = chunked_vector<details::topic_range>;
/**
 * Iterator to iterate over the topic store. This iterate constructs a view into
 * the topic store. The view contains topic namespace and partition statutes
 * range that is valid for the current topic.
 */
class topics_store_iterator
  : public boost::iterator_facade<
      topics_store_iterator,
      const topic_status_view,
      boost::iterators::forward_traversal_tag> {
public:
    topics_store_iterator(
      topic_ranges_t::const_iterator topic_it,
      partition_status_materializing_iterator partitions_it,
      const topics_store* parent);

    size_t index() const;

    bool is_end() const;

private:
    friend class boost::iterator_core_access;

    const topic_status_view& dereference() const;
    void increment();
    bool equal(const topics_store_iterator& other) const;

    mutable std::optional<topic_status_view> _current_value;
    topic_ranges_t::const_iterator _topic_it;
    mutable partition_status_materializing_iterator _partition_it;
    mutable std::optional<partition_status_materializing_iterator>
      _partition_it_next;
    const topics_store* _parent;
};

/**
 * Central part of columnar health report. The topic store compress all topic
 * partition statuses in a partition_status_cstore. It creates a helper data
 * structure to maintain a projection of topic specific partition status ranges
 * in the columnar store. Additionally each topic name is stored without the
 * namespace instead we keep an index into the namespace vector where all
 * namespaces are stored.
 */
class topics_store
  : public serde::
      envelope<topics_store, serde::version<0>, serde::compat_version<0>> {
public:
    topics_store() = default;

    topics_store(topics_store&&) noexcept = default;
    topics_store& operator=(topics_store&&) noexcept = default;
    topics_store& operator=(const topics_store&) = delete;
    topics_store(const topics_store&) = delete;
    ~topics_store() = default;

    void append(
      model::topic_namespace tp_ns,
      const chunked_vector<partition_status>& statuses);

    topics_store_iterator begin() const {
        return {_topics.begin(), _partitions.begin(), this};
    }

    topics_store_iterator end() const {
        return {_topics.end(), _partitions.end(), this};
    }

    auto serde_fields() { return std::tie(_namespaces, _topics, _partitions); }

    size_t size() const { return _topics.size(); }

    topics_store copy() const;

private:
    friend topics_store_iterator;

    const model::ns& ns(size_t idx) const { return _namespaces[idx]; }

    friend bool operator==(const topics_store&, const topics_store&) = default;

    size_t namespace_idx(const model::ns& ns) {
        auto it = std::find(_namespaces.begin(), _namespaces.end(), ns);
        if (it == _namespaces.end()) {
            _namespaces.push_back(ns);
            it = std::prev(_namespaces.end());
        }
        return std::distance(_namespaces.begin(), it);
    }

    std::vector<model::ns> _namespaces;
    chunked_vector<details::topic_range> _topics;
    partition_statuses_cstore _partitions;
};

/**
 * Health report that uses columnar representation of topic partition statuses.
 */
struct columnar_node_health_report
  : public serde::envelope<
      columnar_node_health_report,
      serde::version<0>,
      serde::compat_version<0>> {
    columnar_node_health_report() = default;
    columnar_node_health_report(columnar_node_health_report&&) noexcept
      = default;
    columnar_node_health_report&
    operator=(columnar_node_health_report&&) noexcept
      = default;
    columnar_node_health_report& operator=(const columnar_node_health_report&)
      = delete;
    columnar_node_health_report(const columnar_node_health_report&) = delete;
    ~columnar_node_health_report() = default;

    model::node_id id;
    node::local_state local_state;
    topics_store topics;
    std::optional<drain_manager::drain_status> drain_status;

    /**
     * Materializes columnar representation into the legacy map based health
     * report.
     */
    node_health_report materialize_legacy_report() const;

    columnar_node_health_report copy() const;

    friend bool operator==(
      const columnar_node_health_report&, const columnar_node_health_report&)
      = default;

    auto serde_fields() {
        return std::tie(id, local_state, topics, drain_status);
    }

    friend std::ostream&
    operator<<(std::ostream&, const columnar_node_health_report&);
};

struct cluster_health_report
  : serde::envelope<
      cluster_health_report,
      serde::version<1>,
      serde::compat_version<0>> {
    std::optional<model::node_id> raft0_leader;
    // we split node status from node health reports since node status is a
    // cluster wide property (currently based on raft0 follower state)
    std::vector<node_state> node_states;

    // node reports are node specific information collected directly on a
    // node
    std::vector<node_health_report> node_reports;

    // cluster-wide cached information about total cloud storage usage
    std::optional<size_t> bytes_in_cloud_storage;
    friend std::ostream&
    operator<<(std::ostream&, const cluster_health_report&);

    friend bool
    operator==(const cluster_health_report&, const cluster_health_report&)
      = default;

    auto serde_fields() {
        return std::tie(
          raft0_leader, node_states, node_reports, bytes_in_cloud_storage);
    }
};

struct cluster_health_overview {
    // is healthy is a main cluster indicator, it is intended as an simple flag
    // that will allow all external cluster orchestrating processes to decide if
    // they can proceed with next steps
    bool is_healthy() { return unhealthy_reasons.empty(); }

    // additional human readable information that will make debugging cluster
    // errors easier

    // Zero or more "unhealthy" reasons, which are terse human-readable strings
    // indicating one reason the cluster is unhealthy (there may be several).
    // is_healthy is true iff this list is empty (effectively, is_healthy is
    // redundnat but it's there for backwards compat and convenience).
    std::vector<ss::sstring> unhealthy_reasons;

    // The ID of the controller node, or nullopt if no controller is currently
    // elected.
    std::optional<model::node_id> controller_id;
    // All known nodes in the cluster, including nodes that have joined in the
    // past but are not curently up.
    std::vector<model::node_id> all_nodes;
    // A list of known nodes which are down from the point of view of the health
    // subsystem.
    std::vector<model::node_id> nodes_down;
    // A list of nodes that have been booted up in recovery mode.
    std::vector<model::node_id> nodes_in_recovery_mode;
    std::vector<model::ntp> leaderless_partitions;
    size_t leaderless_count{};
    std::vector<model::ntp> under_replicated_partitions;
    size_t under_replicated_count{};
    std::optional<size_t> bytes_in_cloud_storage;
};

using include_partitions_info = ss::bool_class<struct include_partitions_tag>;

/**
 * Filters are used to limit amout of data returned in health reports
 */
struct partitions_filter
  : serde::
      envelope<partitions_filter, serde::version<0>, serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    using partitions_set_t = absl::node_hash_set<model::partition_id>;
    using topic_map_t = absl::node_hash_map<model::topic, partitions_set_t>;
    using ns_map_t = absl::node_hash_map<model::ns, topic_map_t>;

    bool matches(const model::ntp& ntp) const;
    bool matches(model::topic_namespace_view, model::partition_id) const;

    ns_map_t namespaces;

    friend bool operator==(const partitions_filter&, const partitions_filter&)
      = default;

    auto serde_fields() { return std::tie(namespaces); }
};

struct node_report_filter
  : serde::envelope<
      node_report_filter,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;

    include_partitions_info include_partitions = include_partitions_info::yes;

    partitions_filter ntp_filters;

    friend bool operator==(const node_report_filter&, const node_report_filter&)
      = default;

    friend std::ostream& operator<<(std::ostream&, const node_report_filter&);

    auto serde_fields() { return std::tie(include_partitions, ntp_filters); }
};

struct cluster_report_filter
  : serde::envelope<
      cluster_report_filter,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr int8_t current_version = 0;
    // filtering that will be applied to node reports
    node_report_filter node_report_filter;
    // list of requested nodes, if empty report will contain all nodes
    std::vector<model::node_id> nodes;

    friend std::ostream&
    operator<<(std::ostream&, const cluster_report_filter&);

    friend bool
    operator==(const cluster_report_filter&, const cluster_report_filter&)
      = default;

    auto serde_fields() { return std::tie(node_report_filter, nodes); }
};

using force_refresh = ss::bool_class<struct hm_force_refresh_tag>;

/**
 * RPC requests
 */
using columnar_version = ss::bool_class<struct collect_columnar_tag>;
struct get_node_health_request
  : serde::envelope<
      get_node_health_request,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    node_report_filter filter;
    /**
     * Default to false so that the nodes which does not support the columnar
     * representation of report are automaticaly requesting report in old
     * format.
     */
    columnar_version use_columnar_format = columnar_version::no;

    friend bool
    operator==(const get_node_health_request&, const get_node_health_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const get_node_health_request&);

    auto serde_fields() { return std::tie(filter, use_columnar_format); }
};

struct get_node_health_reply
  : serde::envelope<
      get_node_health_reply,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    errc error = cluster::errc::success;
    std::optional<node_health_report> report;
    std::optional<columnar_node_health_report> columnar_report;

    get_node_health_reply copy() const;

    friend bool
    operator==(const get_node_health_reply&, const get_node_health_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const get_node_health_reply&);

    auto serde_fields() { return std::tie(error, report, columnar_report); }
};

struct get_cluster_health_request
  : serde::envelope<
      get_cluster_health_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t initial_version = 0;
    // version -1: included revision id in partition status
    static constexpr int8_t revision_id_version = -1;
    // version -2: included size_bytes in partition status
    static constexpr int8_t size_bytes_version = -2;

    static constexpr int8_t current_version = size_bytes_version;

    cluster_report_filter filter;
    // if set to true will force node health metadata refresh
    force_refresh refresh = force_refresh::no;
    // this field is not serialized
    int8_t decoded_version = current_version;

    friend bool operator==(
      const get_cluster_health_request&, const get_cluster_health_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const get_cluster_health_request&);

    void serde_write(iobuf& out) {
        using serde::write;
        // the current version decodes into the decoded version and is used in
        // request handling--that is, it is used at layer above serialization so
        // without further changes we'll need to preserve that behavior.
        write(out, current_version);
        write(out, filter);
        write(out, refresh);
    }

    void serde_read(iobuf_parser& in, const serde::header& h) {
        using serde::read_nested;
        decoded_version = read_nested<int8_t>(in, h._bytes_left_limit);
        filter = read_nested<cluster_report_filter>(in, h._bytes_left_limit);
        refresh = read_nested<force_refresh>(in, h._bytes_left_limit);
    }
};

struct get_cluster_health_reply
  : serde::envelope<
      get_cluster_health_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    static constexpr int8_t current_version = 0;

    errc error = cluster::errc::success;
    std::optional<cluster_health_report> report;

    friend bool
    operator==(const get_cluster_health_reply&, const get_cluster_health_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream&, const get_cluster_health_reply&);

    auto serde_fields() { return std::tie(error, report); }
};

} // namespace cluster
