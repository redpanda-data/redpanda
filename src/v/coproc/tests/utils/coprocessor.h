/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "ssx/sformat.h"
#include "storage/record_batch_builder.h"
#include "vassert.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <vector>

struct coprocessor {
    using result = absl::
      flat_hash_map<model::topic, ss::circular_buffer<model::record_batch>>;

    using input_set
      = std::vector<std::pair<model::topic, coproc::topic_ingestion_policy>>;

    coprocessor(coproc::script_id sid, input_set input)
      : _sid(sid)
      , _input_topics(std::move(input)) {
        verify_input_topics();
    }

    virtual ~coprocessor() = default;

    /// \brief The unique identitifer of the coprocessor, this is unique across
    /// all instances of coprocessors of the same kind/type
    coproc::script_id get_script_id() const { return _sid; }

    /// \brief Main method to override, this is the main transform, your logic
    /// goes here
    virtual ss::future<result>
    apply(const model::topic&, ss::circular_buffer<model::record_batch>&&) = 0;

    /// \brief Input topics are static, they can only be set at copro init phase
    const input_set& get_input_topics() const { return _input_topics; }

    /// \brief Detail added for unit testing. Override to let the harness know
    /// about the possible set of solutions (set of output materialized topics)
    /// apply could possibly create/produce onto
    static absl::flat_hash_set<model::topic> output_topics() { return {}; }

private:
    void verify_input_topics() const {
        // Ensure that no duplicates exist
        absl::flat_hash_set<ss::sstring> topics;
        for (const auto& p : _input_topics) {
            const auto& t = p.first;
            vassert(topics.find(t) == topics.end(), "Double topic detected");
            topics.emplace(t);
        }
    }

    /// Unique script ID, coprocessor engine should make sure these are unique
    /// and redpanda should reject registrations from non unique script ids
    coproc::script_id _sid;

    /// Configurable (at construction time) set of topics the coprocessor will
    /// be registered to recieve input from
    input_set _input_topics;
};

/// Produces no data on any materialized topics
struct null_coprocessor : public coprocessor {
    null_coprocessor(coproc::script_id sid, input_set input)
      : coprocessor(sid, std::move(input)) {}

    ss::future<coprocessor::result> apply(
      const model::topic&,
      ss::circular_buffer<model::record_batch>&&) override {
        return ss::make_ready_future<coprocessor::result>();
    }
};

/// Maps all input topics to 1 output topic without modifying the data
struct identity_coprocessor : public coprocessor {
    identity_coprocessor(coproc::script_id sid, input_set input)
      : coprocessor(sid, std::move(input)) {}

    ss::future<coprocessor::result> apply(
      const model::topic&,
      ss::circular_buffer<model::record_batch>&& batches) override {
        coprocessor::result r;
        r.emplace(identity_topic, std::move(batches));
        return ss::make_ready_future<coprocessor::result>(std::move(r));
    }

    static absl::flat_hash_set<model::topic> output_topics() {
        return {identity_topic};
    };

    static const inline model::topic identity_topic = model::topic(
      "identity_topic");
};

/// Maps data from input topic to 1 unique output topic, as the materialized
/// topic name contains the script_id within it
struct unique_identity_coprocessor : public coprocessor {
    unique_identity_coprocessor(coproc::script_id sid, input_set input)
      : coprocessor(sid, std::move(input))
      , _identity_topic(
          model::topic(ssx::sformat("identity_topic_{}", sid()))) {}

    ss::future<coprocessor::result> apply(
      const model::topic&,
      ss::circular_buffer<model::record_batch>&& batches) override {
        coprocessor::result r;
        r.emplace(_identity_topic, std::move(batches));
        return ss::make_ready_future<coprocessor::result>(std::move(r));
    }

private:
    model::topic _identity_topic;
};

struct throwing_coprocessor : public coprocessor {
    throwing_coprocessor(coproc::script_id sid, input_set input)
      : coprocessor(sid, std::move(input)) {}

    ss::future<coprocessor::result> apply(
      const model::topic&,
      ss::circular_buffer<model::record_batch>&& batches) override {
        return ss::make_exception_future<coprocessor::result>(
          std::runtime_error("Coprocessor failed"));
    }
};

struct two_way_split_copro : public coprocessor {
    static const inline model::topic even{model::topic("even")};
    static const inline model::topic odd{model::topic("odd")};

    two_way_split_copro(coproc::script_id sid, input_set input)
      : coprocessor(sid, std::move(input)) {}

    ss::future<coprocessor::result> apply(
      const model::topic& t,
      model::record_batch_reader::data_t&& batches) override {
        storage::record_batch_builder reven(
          model::record_batch_type::raft_data, model::offset{0});
        storage::record_batch_builder rodd(
          model::record_batch_type::raft_data, model::offset{0});
        for (auto& rb : batches) {
            auto o = rb.base_offset();
            rb.for_each_record([&, o](model::record r) {
                if ((o() + r.offset_delta()) % 2 == 0) {
                    reven.add_raw_kw(
                      r.release_key(),
                      r.release_value(),
                      std::move(r.headers()));
                } else {
                    rodd.add_raw_kw(
                      r.release_key(),
                      r.release_value(),
                      std::move(r.headers()));
                }
            });
        }
        auto even_rb = std::move(reven).build();
        auto odd_rb = std::move(rodd).build();
        coprocessor::result r;
        if (!even_rb.empty()) {
            model::record_batch_reader::data_t rb;
            rb.push_back(std::move(even_rb));
            r.emplace(even, std::move(rb));
        }
        if (!odd_rb.empty()) {
            model::record_batch_reader::data_t rb;
            rb.push_back(std::move(odd_rb));
            r.emplace(odd, std::move(rb));
        }
        return ss::make_ready_future<coprocessor::result>(std::move(r));
    }

    static absl::flat_hash_set<model::topic> output_topics() {
        return {even, odd};
    }
};

inline model::topic
to_materialized_topic(const model::topic& src, const model::topic& dest) {
    return model::topic(ssx::sformat("{}._{}_", src(), dest()));
}

namespace coproc::registry {

enum class type_identifier {
    none = 0,
    null_coprocessor,
    identity_coprocessor,
    unique_identity_coprocessor,
    throwing_coprocessor,
    two_way_split_copro
};

inline std::unique_ptr<coprocessor> make_coprocessor(
  type_identifier tid, script_id id, coprocessor::input_set topics) {
    switch (tid) {
    case type_identifier::none:
        return nullptr;
    case type_identifier::null_coprocessor:
        return std::make_unique<null_coprocessor>(id, std::move(topics));
    case type_identifier::identity_coprocessor:
        return std::make_unique<identity_coprocessor>(id, std::move(topics));
    case type_identifier::unique_identity_coprocessor:
        return std::make_unique<unique_identity_coprocessor>(
          id, std::move(topics));
    case type_identifier::throwing_coprocessor:
        return std::make_unique<throwing_coprocessor>(id, std::move(topics));
    case type_identifier::two_way_split_copro:
        return std::make_unique<two_way_split_copro>(id, std::move(topics));
    default:
        return nullptr;
    };
    __builtin_unreachable();
}

} // namespace coproc::registry
