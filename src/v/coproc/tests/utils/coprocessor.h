/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
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
