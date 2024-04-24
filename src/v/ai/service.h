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
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>

namespace ai {
namespace llama {
class backend;
}

struct model;

using model_id = named_type<uuid_t, struct model_id_t>;
using model_name = named_type<ss::sstring, struct model_name_t>;
struct huggingface_file {
    ss::sstring repo;
    ss::sstring filename;

    ss::sstring to_url() const;
};

// A service for interacting with an AI model.
//
// Currently we only support LLMs that have the same architecture as Llama.
class service : public ss::peering_sharded_service<service> {
public:
    struct config {
        std::filesystem::path llm_path;
        std::filesystem::path embeddings_path;
    };

    explicit service(config cfg) noexcept;
    service(const service&) = delete;
    service(service&&) = delete;
    service& operator=(const service&) = delete;
    service& operator=(service&&) = delete;
    ~service() noexcept;

    ss::future<> start();

    ss::future<model_id> deploy_embeddings_model(ss::sstring url);
    ss::future<model_id> deploy_text_generation_model(ss::sstring url);
    ss::future<model_id> deploy_embeddings_model(const huggingface_file& hff) {
        return deploy_embeddings_model(hff.to_url());
    }
    ss::future<model_id>
    deploy_text_generation_model(const huggingface_file& hff) {
        return deploy_text_generation_model(hff.to_url());
    }
    ss::future<absl::flat_hash_map<model_id, model_name>> list_models();
    ss::future<> delete_model(model_id url);

    ss::future<> stop();

    struct generate_text_options {
        int32_t max_tokens;
    };

    // Compute embeddings based on the prompt.
    ss::future<std::vector<float>>
    compute_embeddings(model_id, ss::sstring text);

    // Generate text based on the prompt.
    ss::future<ss::sstring>
    generate_text(model_id, ss::sstring prompt, generate_text_options);

private:
    ss::future<absl::flat_hash_map<model_id, model_name>> do_list_models();
    ss::future<std::vector<float>>
    do_compute_embeddings(model_id, ss::sstring prompt);
    ss::future<ss::sstring>
    do_generate_text(model_id, ss::sstring prompt, generate_text_options opts);

    ss::future<> load_llm(const ss::sstring& filename);
    ss::future<> load_embeddings_model(const ss::sstring& filename);

    config _config;
    std::unique_ptr<llama::backend> _backend;
    absl::flat_hash_map<model_id, std::unique_ptr<model>> _models;
};

} // namespace ai
