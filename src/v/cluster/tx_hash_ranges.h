/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "hashing/murmur.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "reflection/adl.h"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

namespace cluster {

using tx_hash_type = uint32_t;
constexpr tx_hash_type tx_tm_hash_max
  = std::numeric_limits<tx_hash_type>::max();

static tx_hash_type get_default_range_size(int32_t tm_partitions_amount) {
    // integer division acts as div
    // size of last range can be larger than default range size
    // last range covers hashes up to tx_tm_hash_max
    return tx_tm_hash_max / tm_partitions_amount;
}

// Return hash of tx_id in range [0, tx_tm_hash_max]
inline tx_hash_type get_tx_id_hash(const kafka::transactional_id& tx_id) {
    return murmur2(ss::sstring(tx_id).data(), ss::sstring(tx_id).size());
}

enum class tx_hash_ranges_errc {
    success = 0,
    not_hosted = 1,
    intersection = 2
};

// Defines hash range as section where both ends are included
struct tx_hash_range
  : serde::
      envelope<tx_hash_range, serde::version<0>, serde::compat_version<0>> {
    tx_hash_type first;
    tx_hash_type last;

    tx_hash_range() = default;

    tx_hash_range(tx_hash_type f, tx_hash_type l)
      : first(f)
      , last(l) {}

    auto serde_fields() { return std::tie(first, last); }

    bool contains(tx_hash_type v) const { return v >= first && v <= last; }

    bool contains(tx_hash_range r) const {
        return r.first >= first && r.last <= last;
    }

    bool intersects(tx_hash_range r) const {
        return (r.first >= first && r.first <= last)
               || (r.last >= first && r.last <= last) || r.contains(*this);
    }
};

inline tx_hash_range default_hash_range(
  model::partition_id tm_partition, int32_t partitions_amount) {
    tx_hash_type range_size = get_default_range_size(partitions_amount);
    tx_hash_type hash_range_begin = range_size * tm_partition;
    tx_hash_type hash_range_end = (tm_partition + 1) == partitions_amount
                                    ? tx_tm_hash_max
                                    : (range_size * (tm_partition + 1)) - 1;
    return {hash_range_begin, hash_range_end};
}

struct tx_hash_ranges_set
  : serde::envelope<
      tx_hash_ranges_set,
      serde::version<0>,
      serde::compat_version<0>> {
    std::vector<tx_hash_range> ranges{};

    tx_hash_ranges_set() = default;
    tx_hash_ranges_set(std::vector<tx_hash_range>&& hr)
      : ranges(hr) {}

    auto serde_fields() { return std::tie(ranges); }

    void add_range(tx_hash_range range) {
        auto merged_to = ranges.end();
        if (range.first != 0) {
            for (auto r_it = ranges.begin(); r_it != ranges.end(); ++r_it) {
                if (r_it->last == range.first - 1) {
                    r_it->last = range.last;
                    merged_to = r_it;
                    break;
                }
            }
        }
        if (range.last != tx_tm_hash_max) {
            for (auto r_it = ranges.begin(); r_it != ranges.end(); ++r_it) {
                if (r_it->first == range.last + 1) {
                    if (merged_to != ranges.end()) {
                        merged_to->last = r_it->last;
                        ranges.erase(r_it);
                    } else {
                        r_it->first = range.first;
                        merged_to = r_it;
                    }
                    break;
                }
            }
        }
        if (merged_to == ranges.end()) {
            ranges.push_back(range);
        }
    }

    bool contains(tx_hash_type v) const {
        for (const auto& r : ranges) {
            if (r.contains(v)) {
                return true;
            }
        }
        return false;
    }

    bool contains(const tx_hash_range& r) const {
        for (const auto& range : ranges) {
            if (range.contains(r)) {
                return true;
            }
        }
        return false;
    }

    bool intersects(tx_hash_range range) const {
        return std::any_of(
          ranges.begin(), ranges.end(), [range1 = range](tx_hash_range range2) {
              return range1.intersects(range2);
          });
    }
};

using repartitioning_id = named_type<int64_t, struct repartitioning_id_type>;

struct draining_txs
  : serde::envelope<draining_txs, serde::version<0>, serde::compat_version<0>> {
    repartitioning_id id;
    tx_hash_ranges_set ranges{};
    absl::btree_set<kafka::transactional_id> transactions{};

    draining_txs() = default;

    draining_txs(
      repartitioning_id id,
      tx_hash_ranges_set ranges,
      absl::btree_set<kafka::transactional_id> txs)
      : id(id)
      , ranges(std::move(ranges))
      , transactions(std::move(txs)) {}

    auto serde_fields() { return std::tie(id, ranges, transactions); }

    bool contains(kafka::transactional_id tx_id) {
        return transactions.contains(tx_id);
    }

    bool contains(tx_hash_type hash) { return ranges.contains(hash); }
};

namespace hosted_transactions {
template<class T>
tx_hash_ranges_errc
exclude_transaction(T& self, const kafka::transactional_id& tx_id) {
    if (self.excluded_transactions.contains(tx_id)) {
        return tx_hash_ranges_errc::success;
    }
    if (self.included_transactions.contains(tx_id)) {
        self.included_transactions.erase(tx_id);
        vassert(
          !self.hash_ranges.contains(get_tx_id_hash(tx_id)),
          "hash_ranges must not contain included txes");
        return tx_hash_ranges_errc::success;
    }
    tx_hash_type hash = get_tx_id_hash(tx_id);
    if (!self.hash_ranges.contains(hash)) {
        return tx_hash_ranges_errc::not_hosted;
    }
    self.excluded_transactions.insert(tx_id);
    return tx_hash_ranges_errc::success;
}

template<class T>
tx_hash_ranges_errc
include_transaction(T& self, const kafka::transactional_id& tx_id) {
    if (self.included_transactions.contains(tx_id)) {
        return tx_hash_ranges_errc::success;
    }
    if (self.excluded_transactions.contains(tx_id)) {
        self.excluded_transactions.erase(tx_id);
        vassert(
          self.hash_ranges.contains(get_tx_id_hash(tx_id)),
          "hash_ranges must not contain excluded txes");
        return tx_hash_ranges_errc::success;
    }
    tx_hash_type hash = get_tx_id_hash(tx_id);
    if (self.hash_ranges.contains(hash)) {
        return tx_hash_ranges_errc::intersection;
    }
    self.included_transactions.insert(tx_id);
    return tx_hash_ranges_errc::success;
}

template<class T>
tx_hash_ranges_errc add_range(T& self, const tx_hash_range& range) {
    if (self.hash_ranges.intersects(range)) {
        return tx_hash_ranges_errc::intersection;
    }
    for (const auto& tx_id : self.excluded_transactions) {
        tx_hash_type hash = get_tx_id_hash(tx_id);
        if (range.contains(hash)) {
            self.excluded_transactions.erase(tx_id);
        }
    }
    for (const auto& tx_id : self.included_transactions) {
        tx_hash_type hash = get_tx_id_hash(tx_id);
        if (range.contains(hash)) {
            self.included_transactions.erase(tx_id);
        }
    }
    self.hash_ranges.add_range(range);
    return tx_hash_ranges_errc::success;
}

template<class T>
bool contains(T& self, const kafka::transactional_id& tx_id) {
    if (self.excluded_transactions.contains(tx_id)) {
        return false;
    }
    if (self.included_transactions.contains(tx_id)) {
        return true;
    }
    auto tx_id_hash = get_tx_id_hash(tx_id);
    return self.hash_ranges.contains(tx_id_hash);
}
} // namespace hosted_transactions

} // namespace cluster

namespace reflection {

template<>
struct adl<cluster::tx_hash_range> {
    void to(iobuf& out, cluster::tx_hash_range&& hr) {
        reflection::serialize(out, hr.first, hr.last);
    }
    cluster::tx_hash_range from(iobuf_parser& in) {
        auto first = reflection::adl<cluster::tx_hash_type>{}.from(in);
        auto last = reflection::adl<cluster::tx_hash_type>{}.from(in);
        return {first, last};
    }
};

template<>
struct adl<cluster::tx_hash_ranges_set> {
    void to(iobuf& out, cluster::tx_hash_ranges_set&& hr) {
        reflection::serialize(out, hr.ranges);
    }
    cluster::tx_hash_ranges_set from(iobuf_parser& in) {
        auto ranges
          = reflection::adl<std::vector<cluster::tx_hash_range>>{}.from(in);
        return {std::move(ranges)};
    }
};

template<>
struct adl<cluster::draining_txs> {
    void to(iobuf& out, cluster::draining_txs&& dr) {
        reflection::serialize(out, dr.id, dr.ranges, dr.transactions);
    }
    cluster::draining_txs from(iobuf_parser& in) {
        auto id = reflection::adl<cluster::repartitioning_id>{}.from(in);
        auto ranges
          = reflection::adl<std::vector<cluster::tx_hash_range>>{}.from(in);
        auto txs = reflection::adl<absl::btree_set<kafka::transactional_id>>{}
                     .from(in);
        return {id, std::move(ranges), std::move(txs)};
    }
};

} // namespace reflection
