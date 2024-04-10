/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace ai {

class model;

// A service for interacting with an AI model.
//
// Currently we only support LLMs that have the same architecture as Llama.
class service : public ss::peering_sharded_service<service> {
public:
    service() noexcept;
    service(const service&) = delete;
    service(service&&) = delete;
    service& operator=(const service&) = delete;
    service& operator=(service&&) = delete;
    ~service() noexcept;

    struct config {
        std::filesystem::path model_file;
    };

    ss::future<> start(config);
    ss::future<> stop();

    struct generate_text_options {
        int32_t max_tokens;
    };

    // Generate text based on the prompt.
    ss::future<std::vector<float>> compute_embeddings(ss::sstring prompt);

private:
    ss::future<std::vector<float>> do_compute_embeddings(ss::sstring prompt);

    std::unique_ptr<model> _model;
};

} // namespace ai
