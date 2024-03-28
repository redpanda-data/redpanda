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

#include "utils/named_type.h"

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include <boost/range/irange.hpp>

#include <filesystem>
#include <ggml.h>
#include <llama.h>
#include <memory>

namespace ai::llama {

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

using token = named_type<llama_token, struct token_tag>;
using position = named_type<llama_pos, struct pos_tag>;

class batch;
class batch_view;
class token_vector;

// Representation of a LLM with the llama architecture.
class model {
public:
    using underlying = handle<llama_model, llama_free_model>;
    using context = handle<llama_context, llama_free>;

    // Tokenize a prompt into tokens.
    std::vector<token> tokenize(const seastar::sstring& prompt) const noexcept;

    // Decode the token and append it to `output`.
    void append_decoded_token(token id, seastar::sstring* output) const;

    // Decode this batch
    //
    // @returns true if success, false if batch too big
    //
    // @throws if there is an error
    bool decode(batch_view);

    // Get the logits vector (which is the size of the vocab) for this position.
    std::span<float> get_logits(position);

    // The context size to use during inference.
    int32_t n_ctx() const;
    int32_t n_batch() const;

    // TODO: This should probably be a vocab struct.
    uint32_t n_vocab() const;
    // Get all tokens in the vocab.
    boost::integer_range<token> vocab() const;
    // Get the end of sentence token for the vocab.
    token eos() const;

    // Reset this model's internal state so that a new prompt can be
    // evaluated/decoded.
    void reset();
    // Reset only a sequence
    void reset(uint32_t seq_id);

private:
    model() = default;
    friend class backend;

    underlying _underlying;
    context _context;
};

// A backend for AI resources.
class backend {
public:
    backend() = default;
    backend(const backend&) = delete;
    backend& operator=(const backend&) = delete;
    backend(backend&&) = default;
    backend& operator=(backend&&) = default;
    ~backend() = default;

    void initialize();
    void shutdown();

    /* Load a model from a file on the filesystem (using mmap). */
    model load(const std::filesystem::path& model_file);
};

// A vector of tokens with helpers to sample the vector.
class token_vector {
public:
    void add(token t, float logit, float p) {
        _tokens.emplace_back(t, logit, p);
    }
    void reserve(size_t n) { _tokens.reserve(n); }
    void clear() { _tokens.clear(); }

    token greedy_sample() const;

private:
    friend class model;

    std::vector<llama_token_data> _tokens;
};

/**
 * A batch to submit to the LLM and process all at once.
 */
class batch {
public:
    struct config {
        int32_t n_tokens;
        int32_t embd;
        int32_t n_seq_max;
    };
    batch();
    explicit batch(config cfg);
    batch(const batch&) = delete;
    batch& operator=(const batch&) = delete;
    batch(batch&& other) noexcept;
    batch& operator=(batch&& other) noexcept;
    ~batch();

    // Add a token to this batch
    void
    add(token id, position pos, std::span<llama_seq_id> seq_ids, bool logits);
    // The number of tokens in this batch.
    int32_t size() const;
    // Mark this position as needed logits computed.
    void compute_logits(position pos);
    void clear();

    batch_view subspan(position start, uint32_t size) const;

private:
    friend class model;
    friend class batch_view;

    llama_batch _underlying;
    int32_t _max_tokens;
};

class batch_view {
public:
    batch_view(const batch&, position, uint32_t);

private:
    friend class model;
    llama_batch _underlying;
};

} // namespace ai::llama
