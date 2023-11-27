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

#include <absl/container/node_hash_set.h>

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

namespace cluster {

using tx_id_hash = named_type<uint32_t, struct tx_hash_type_tag>;

static tx_id_hash get_default_tx_hash_range_size(int32_t tm_partitions_amount) {
    // integer division acts as div
    // size of last range can be larger than default range size
    // last range covers hashes up to tx_tm_hash_max
    return tx_id_hash(tx_id_hash::max() / tm_partitions_amount);
}

// Return hash of tx_id in range [0, tx_tm_hash_max]
inline tx_id_hash get_tx_id_hash(const kafka::transactional_id& tx_id) {
    auto copy = ss::sstring(tx_id);
    return tx_id_hash(murmur2(copy.data(), copy.size()));
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
    tx_id_hash first;
    tx_id_hash last;

    tx_hash_range() = default;

    tx_hash_range(tx_id_hash f, tx_id_hash l)
      : first(f)
      , last(l) {}

    friend bool operator==(const tx_hash_range&, const tx_hash_range&)
      = default;

    auto serde_fields() { return std::tie(first, last); }

    bool contains(tx_id_hash v) const { return v >= first && v <= last; }

    bool contains(tx_hash_range r) const {
        return r.first >= first && r.last <= last;
    }

    bool intersects(tx_hash_range r) const {
        return (r.first >= first && r.first <= last)
               || (r.last >= first && r.last <= last) || r.contains(*this);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const tx_hash_range& range) {
        fmt::print(o, "[{}, {}]", range.first, range.last);
        return o;
    }
};

inline tx_hash_range
default_hash_range(model::partition_id tm_partition, int32_t partition_cnt) {
    const tx_id_hash range_size = get_default_tx_hash_range_size(partition_cnt);

    tx_id_hash hash_range_end(
      (tm_partition + 1) == partition_cnt
        ? tx_id_hash::max()
        : (range_size * (tm_partition + 1)) - 1);

    return {tx_id_hash(range_size * tm_partition), hash_range_end};
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

    friend bool operator==(const tx_hash_ranges_set&, const tx_hash_ranges_set&)
      = default;

    auto serde_fields() { return std::tie(ranges); }

    void add_range(tx_hash_range range) {
        auto merged_to = ranges.end();
        if (range.first != tx_id_hash(0)) {
            for (auto r_it = ranges.begin(); r_it != ranges.end(); ++r_it) {
                if (r_it->last == range.first - 1) {
                    r_it->last = range.last;
                    merged_to = r_it;
                    break;
                }
            }
        }
        if (range.last != tx_id_hash::max()) {
            for (auto r_it = ranges.begin(); r_it != ranges.end(); ++r_it) {
                if (r_it->first == range.last + tx_id_hash(1)) {
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

    bool contains(tx_id_hash v) const {
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

    friend std::ostream&
    operator<<(std::ostream& o, const tx_hash_ranges_set& ranges) {
        fmt::print(o, "{{ {} }}", ranges.ranges);
        return o;
    }
};

using repartitioning_id = named_type<int64_t, struct repartitioning_id_type>;

struct hosted_txs
  : serde::envelope<hosted_txs, serde::version<0>, serde::compat_version<0>> {
    tx_hash_ranges_set hash_ranges{};
    absl::node_hash_set<kafka::transactional_id> excluded_transactions{};
    absl::node_hash_set<kafka::transactional_id> included_transactions{};

    hosted_txs() = default;

    friend bool operator==(const hosted_txs&, const hosted_txs&) = default;

    hosted_txs(
      tx_hash_ranges_set ranges,
      absl::node_hash_set<kafka::transactional_id> excluded_txs,
      absl::node_hash_set<kafka::transactional_id> included_txs)
      : hash_ranges(std::move(ranges))
      , excluded_transactions(std::move(excluded_txs))
      , included_transactions(std::move(included_txs)) {}

    auto serde_fields() {
        return std::tie(
          hash_ranges, excluded_transactions, included_transactions);
    }

    friend std::ostream& operator<<(std::ostream& o, hosted_txs h) {
        fmt::print(
          o,
          "{{ ranges: {}, excluded: {}, included: {} }}",
          h.hash_ranges,
          h.excluded_transactions.size(),
          h.included_transactions.size());
        return o;
    }
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
    tx_id_hash hash = get_tx_id_hash(tx_id);
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
          "hash_ranges must contain excluded txes");
        return tx_hash_ranges_errc::success;
    }
    tx_id_hash hash = get_tx_id_hash(tx_id);
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
        tx_id_hash hash = get_tx_id_hash(tx_id);
        if (range.contains(hash)) {
            self.excluded_transactions.erase(tx_id);
        }
    }
    for (const auto& tx_id : self.included_transactions) {
        tx_id_hash hash = get_tx_id_hash(tx_id);
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
        auto first = reflection::adl<cluster::tx_id_hash>{}.from(in);
        auto last = reflection::adl<cluster::tx_id_hash>{}.from(in);
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

} // namespace reflection
