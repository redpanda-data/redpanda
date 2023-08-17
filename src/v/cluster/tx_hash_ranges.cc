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

#include "cluster/tx_hash_ranges.h"

namespace cluster {

std::ostream& operator<<(std::ostream& o, const tx_hash_ranges_errc& ec) {
    switch (ec) {
    case tx_hash_ranges_errc::success:
        o << "tx_hash_ranges_errc::success";
        break;
    case tx_hash_ranges_errc::not_hosted:
        o << "tx_hash_ranges_errc::not_hosted";
        break;
    case tx_hash_ranges_errc::intersection:
        o << "tx_hash_ranges_errc::intersection";
        break;
    }
    return o;
}

std::ostream& operator<<(std::ostream& o, const tx_hash_range& r) {
    fmt::print(o, "{{tx_hash_range first:{} last:{}}}", r.first, r.last);
    return o;
}

} // namespace cluster

namespace reflection {

void adl<cluster::tx_hash_range>::to(iobuf& out, cluster::tx_hash_range&& hr) {
    reflection::serialize(out, hr.first, hr.last);
}

cluster::tx_hash_range adl<cluster::tx_hash_range>::from(iobuf_parser& in) {
    auto first = reflection::adl<cluster::tx_hash_type>{}.from(in);
    auto last = reflection::adl<cluster::tx_hash_type>{}.from(in);
    return {first, last};
}

void adl<cluster::tx_hash_ranges_set>::to(
  iobuf& out, cluster::tx_hash_ranges_set&& hr) {
    reflection::serialize(out, hr.ranges);
}

cluster::tx_hash_ranges_set
adl<cluster::tx_hash_ranges_set>::from(iobuf_parser& in) {
    auto ranges = reflection::adl<std::vector<cluster::tx_hash_range>>{}.from(
      in);
    return {std::move(ranges)};
}

void adl<cluster::draining_txs>::to(iobuf& out, cluster::draining_txs&& dr) {
    reflection::serialize(out, dr.id, dr.ranges, dr.transactions);
}

cluster::draining_txs adl<cluster::draining_txs>::from(iobuf_parser& in) {
    auto id = reflection::adl<cluster::repartitioning_id>{}.from(in);
    auto ranges = reflection::adl<std::vector<cluster::tx_hash_range>>{}.from(
      in);
    auto txs = reflection::adl<absl::btree_set<kafka::transactional_id>>{}.from(
      in);
    return {id, std::move(ranges), std::move(txs)};
}

} // namespace reflection
