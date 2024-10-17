
// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iostream.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/group_configuration.h"
#include "raft/state_machine_manager.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "storage/record_batch_builder.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/randoms.h"
#include "test_utils/test.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <ostream>

using namespace raft;
namespace {
/**
 * We use value entry struct to make kv_store apply operations not
 * idempotent
 **/
struct value_entry
  : serde::envelope<value_entry, serde::version<0>, serde::compat_version<0>> {
    value_entry() = default;

    explicit value_entry(ss::sstring v)
      : value(std::move(v)) {}

    ss::sstring value;
    size_t update_cnt{0};
    friend bool operator==(const value_entry&, const value_entry&) = default;

    auto serde_fields() { return std::tie(value, update_cnt); }

    friend std::ostream& operator<<(std::ostream& o, const value_entry& ve) {
        fmt::print(o, "{{value: {}, update_cnt: {}}}", ve.value, ve.update_cnt);
        return o;
    }
};
} // namespace

/**
 * Simple kv-store state machine
 */
template<typename BaseT = state_machine_base>
struct simple_kv_base : public BaseT {
    using state_t = absl::flat_hash_map<ss::sstring, value_entry>;
    static constexpr std::string_view name = "simple_kv";

    explicit simple_kv_base(raft_node_instance& rn)
      : raft_node(rn) {}

    static void apply_to_state(
      const model::record_batch& batch,
      absl::flat_hash_map<ss::sstring, value_entry>& state) {
        if (batch.header().type != model::record_batch_type::raft_data) {
            return;
        }
        batch.for_each_record([&state](model::record r) {
            auto k = serde::from_iobuf<ss::sstring>(r.key().copy());
            auto v = serde::from_iobuf<std::optional<ss::sstring>>(
              r.value().copy());
            if (v) {
                auto [it, success] = state.try_emplace(k, *v);
                if (!success) {
                    it->second.value = std::move(*v);
                    it->second.update_cnt++;
                }
            } else {
                state.erase(k);
            }
        });
    }
    ss::future<> start() override { return ss::now(); }
    ss::future<> stop() override { co_await BaseT::stop(); };

    ss::future<> apply(
      const model::record_batch& batch, const ssx::semaphore_units&) override {
        apply_to_state(batch, state);
        co_return;
    }

    ss::future<> apply_raft_snapshot(const iobuf& buffer) override {
        state = serde::from_iobuf<state_t>(buffer.copy());
        co_return;
    };

    size_t get_local_state_size() const final { return 0; }
    ss::future<> remove_local_state() final { co_return; }

    state_t state;
    raft_node_instance& raft_node;
};
class simple_kv : public simple_kv_base<state_machine_base> {
public:
    explicit simple_kv(raft_node_instance& rn)
      : simple_kv_base<>(rn) {}

    ss::future<iobuf>
    take_snapshot(model::offset last_included_offset) override {
        state_t inc_state;
        // build incremental snapshot
        auto snap = co_await raft_node.raft()->open_snapshot();
        auto start_offset = raft_node.raft()->start_offset();
        if (snap) {
            auto data = co_await read_iobuf_exactly(
              snap->reader.input(), co_await snap->reader.get_snapshot_size());
            inc_state = serde::from_iobuf<state_t>(std::move(data));
        }

        auto rdr = co_await raft_node.raft()->make_reader(
          storage::log_reader_config(
            start_offset, last_included_offset, ss::default_priority_class()));

        auto batches = co_await model::consume_reader_to_memory(
          std::move(rdr), default_timeout());

        std::for_each(
          batches.begin(),
          batches.end(),
          [&inc_state](const model::record_batch& b) {
              return simple_kv_base::apply_to_state(b, inc_state);
          });

        co_return serde::to_iobuf(std::move(inc_state));
    };
};
using wait_for_each_batch = ss::bool_class<struct wait_for_each_tag>;

struct state_machine_fixture : raft_fixture {
    ss::future<result<raft::replicate_result>>
    upsert(ss::sstring k, ss::sstring v) {
        return replicate({std::make_pair(std::move(k), std::move(v))});
    }

    ss::future<result<raft::replicate_result>> remove(ss::sstring k) {
        return replicate({std::make_pair(std::move(k), std::nullopt)});
    }

    ss::future<result<raft::replicate_result>> replicate(
      std::vector<std::pair<ss::sstring, std::optional<ss::sstring>>> values) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));
        for (auto& [k, v] : values) {
            builder.add_raw_kv(serde::to_iobuf(k), serde::to_iobuf(v));
        }

        co_return co_await retry_with_leader(
          10s + model::timeout_clock::now(),
          [b = std::move(builder).build()](
            raft_node_instance& leader_node) mutable {
              return leader_node.raft()->replicate(
                model::make_memory_record_batch_reader(b.share()),
                raft::replicate_options(raft::consistency_level::quorum_ack));
          });
    }

    ss::future<absl::flat_hash_map<ss::sstring, value_entry>>
    build_random_state(
      int op_cnt,
      wait_for_each_batch wait_for_each = wait_for_each_batch::no,
      size_t max_batch_size = 50) {
        absl::flat_hash_map<ss::sstring, value_entry> state;

        for (int i = 0; i < op_cnt;) {
            const auto batch_sz = random_generators::get_int<size_t>(
              1, max_batch_size);
            std::vector<std::pair<ss::sstring, std::optional<ss::sstring>>> ops;
            for (auto n = 0; n < batch_sz; ++n) {
                auto k = random_generators::gen_alphanum_string(10);
                auto v = random_generators::gen_alphanum_string(10);

                if (state.empty() || tests::random_bool()) {
                    // add
                    auto [it, success] = state.try_emplace(k, v);
                    if (!success) {
                        it->second.update_cnt++;
                    }
                    ops.emplace_back(k, v);
                } else {
                    auto idx = random_generators::get_int<size_t>(
                      0, state.size() - 1);
                    auto it = std::next(state.begin(), idx);

                    if (tests::random_bool()) {
                        // remove
                        ops.emplace_back(it->first, std::nullopt);
                        state.erase(it);
                    } else {
                        // replace
                        ops.emplace_back(it->first, v);
                        it->second.value = v;
                        it->second.update_cnt++;
                    }
                }
            }
            i += batch_sz;

            auto result = co_await replicate(std::move(ops));
            if (result.has_value()) {
                vlog(
                  logger().debug,
                  "replication result: [last_offset: {}]",
                  result.value().last_offset);
            } else {
                auto error = result.error();
                vlog(logger().warn, "replication failure: {}", error);
                throw std::runtime_error(
                  fmt::format("replication failure {}", error));
            }

            if (wait_for_each) {
                co_await wait_for_apply();
            }
        }
        co_return state;
    }

    ss::future<> wait_for_apply() {
        auto committed_offset = co_await with_leader(
          10s, [](raft_node_instance& node) {
              return node.raft()->committed_offset();
          });

        co_await parallel_for_each_node(
          [committed_offset](raft_node_instance& node) {
              return node.raft()->stm_manager()->wait(
                committed_offset, default_timeout());
          });
    }

    void create_nodes() {
        for (auto i = 0; i < 3; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }
    }
};
