// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/loop.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/range/irange.hpp>
#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

SEASTAR_THREAD_TEST_CASE(test_sharded_store_cross_shard_def) {
    constexpr size_t id_n = 1000;
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store] { store.stop().get(); });

    const pps::schema_version ver1{1};

    // Upsert a large(ish) number of schemas to the store, all with different
    // subject names and IDs, so they should hash to different shards.
    for (int i = 1; i <= id_n; ++i) {
        auto referenced_schema = pps::canonical_schema{
          pps::subject{fmt::format("simple_{}.proto", i)}, simple.share()};
        store
          .upsert(
            pps::seq_marker{
              std::nullopt,
              std::nullopt,
              ver1,
              pps::seq_marker_key_type::schema},
            referenced_schema.copy(),
            pps::schema_id{i},
            ver1,
            pps::is_deleted::no)
          .get();
    }

    BOOST_REQUIRE(store.has_schema(pps::schema_id{id_n}).get());

    // for each upserted schema, submit a number of concurrent
    // get_schema_definition requests, spreading the requests across cores. the
    // goal here is to have copies of a particular schema live on shards other
    // than the "home" shard of that schema. previously, when
    // 'store::get_schema_definition' was implemented with an iobuf share at the
    // interface, this loop would reliably lead to a UAF segfault in
    // '~ss::deleter()', the refcounted, non-thread-safe deleter for the
    // temporary buffers underlying the shared iobuf.
    for (int i = 1; i <= id_n; ++i) {
        constexpr int num_parallel_requests = 20;
        ss::parallel_for_each(
          boost::irange(0, num_parallel_requests),
          [&store, i](auto shrd) {
              return ss::smp::submit_to(shrd % ss::smp::count, [&store, i]() {
                  return store.get_schema_definition(pps::schema_id{i})
                    .discard_result();
              });
          })
          .get();
    }
}
