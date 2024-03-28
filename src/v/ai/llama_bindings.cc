/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "ai/llama_bindings.h"

#include "base/vassert.h"
#include "base/vlog.h"

#include <seastar/util/log.hh>

#include <absl/strings/ascii.h>

#include <algorithm>
#include <llama.h>
#include <thread>

namespace ai::llama {

namespace {

// NOLINTNEXTLINE
static seastar::logger logger("ai/llama");

model::underlying load_underlying(const std::filesystem::path& model_file) {
    llama_model_params model_params = llama_model_default_params();
    model_params.n_gpu_layers = std::numeric_limits<int32_t>::max();
    model_params.progress_callback_user_data = nullptr;
    model_params.progress_callback =
      [](float progress, void* user_data) -> bool {
        std::ignore = user_data;
        vlog(logger.debug, "Loading model: {}%", progress * 100.0);
        // TODO: Return false to stop loading via abort source.
        return true;
    };
    model_params.use_mmap = true;   // How the model is loaded
    model_params.use_mlock = false; // Force the model to stay in RAM
    // TODO: Evaluate the rest of the parameters
    model::underlying model{
      llama_load_model_from_file(model_file.native().c_str(), model_params)};
    if (!model) {
        throw std::runtime_error(
          seastar::format("unable to load model at: {}", model_file.native()));
    }
    return model;
}

model::context initialize_context(llama_model* model) {
    llama_context_params ctx_params = llama_context_default_params();

    ctx_params.seed = -1; // Use RNG
    // ctx_params.n_ctx = 0; // Use the model's context window
    ctx_params.n_threads = std::thread::hardware_concurrency();
    ctx_params.n_threads_batch = ctx_params.n_threads; // use n_threads
    // TODO: Figure out the other parameters here.
    model::context ctx{llama_new_context_with_model(model, ctx_params)};
    if (!ctx) {
        throw std::runtime_error("unable to initialize model context");
    }
    return ctx;
}
} // namespace

bool model::decode(batch_view b) {
    int32_t result = llama_decode(_context.get(), b._underlying);
    if (result < 0) {
        throw std::runtime_error(
          ss::format("failure to decode tokens: {}", result));
    }
    return result == 0;
}

std::vector<token>
model::tokenize(const seastar::sstring& prompt) const noexcept {
    constexpr bool add_bos = true;
    constexpr bool add_special = false;
    size_t max_tokens = prompt.size();
    if (add_bos) {
        ++max_tokens;
    }
    std::vector<token> result;
    static_assert(
      sizeof(token) == sizeof(llama_token), "needed for casts below");
    result.reserve(max_tokens);
    int32_t n_tokens = llama_tokenize(
      _underlying.get(),
      prompt.data(),
      int32_t(prompt.size()),
      reinterpret_cast<llama_token*>(result.data()), // NOLINT
      int32_t(result.size()),
      add_bos,
      add_special);
    if (n_tokens < 0) {
        result.resize(-n_tokens);
        int32_t resized = llama_tokenize(
          _underlying.get(),
          prompt.data(),
          int32_t(prompt.size()),
          reinterpret_cast<llama_token*>(result.data()), // NOLINT
          int32_t(result.size()),
          add_bos,
          add_special);
        vassert(
          resized == -n_tokens,
          "expected {} tokens when tokenizing, but got {}",
          -n_tokens,
          resized);
    } else {
        result.resize(n_tokens);
    }
    return result;
};

void model::append_decoded_token(token id, seastar::sstring* output) const {
    static constexpr size_t decoded_guess_size = 8;
    std::array<char, decoded_guess_size> decoded; // NOLINT
    int32_t result = llama_token_to_piece(
      _underlying.get(), id, decoded.data(), decoded.size());
    if (result >= 0) {
        output->append(decoded.data(), result);
        return;
    }
    seastar::sstring decoded_str(
      seastar::sstring::initialized_later(), -result);
    int32_t resized = llama_token_to_piece(
      _underlying.get(), id, decoded_str.data(), int32_t(decoded_str.size()));
    vassert(
      resized == -result,
      "expected string of length {} when decoding, but got {}",
      -result,
      resized);
    output->append(decoded_str.data(), decoded_str.size());
}

model backend::load(const std::filesystem::path& model_file) {
    model m;
    m._underlying = load_underlying(model_file);
    m._context = initialize_context(m._underlying.get());
    return m;
}

void batch::add(
  token id, position pos, std::span<llama_seq_id> seq_ids, bool logits) {
    // TODO: Realloc if this happens
    vassert(
      pos >= 0 && pos < _max_tokens,
      "out of bounds: 0 <= {} < {}",
      pos,
      _max_tokens);
    // NOLINTBEGIN(*-pointer-arithmetic)
    _underlying.token[_underlying.n_tokens] = id;
    _underlying.pos[_underlying.n_tokens] = pos;
    _underlying.n_seq_id[_underlying.n_tokens] = static_cast<int32_t>(
      seq_ids.size());
    for (size_t i = 0; i < seq_ids.size(); ++i) {
        _underlying.seq_id[_underlying.n_tokens][i] = seq_ids[i];
    }
    _underlying.logits[_underlying.n_tokens] = logits ? 1 : 0;

    _underlying.n_tokens++;
    // NOLINTEND(*-pointer-arithmetic)
}

void batch::compute_logits(position pos) {
    // NOLINTBEGIN(*-pointer-arithmetic)
    _underlying.logits[pos] = true;
    // NOLINTEND(*-pointer-arithmetic)
}
int32_t batch::size() const { return _underlying.n_tokens; }
void batch::clear() { _underlying.n_tokens = 0; }

std::span<float> model::get_logits(position pos) {
    return {llama_get_logits_ith(_context.get(), pos), n_vocab()};
}
uint32_t model::n_vocab() const { return llama_n_vocab(_underlying.get()); }
int32_t model::n_ctx() const { return int32_t(llama_n_ctx(_context.get())); }
int32_t model::n_batch() const {
    return int32_t(llama_n_batch(_context.get()));
}
token model::eos() const { return token(llama_token_eos(_underlying.get())); }

boost::integer_range<token> model::vocab() const {
    return boost::irange(token(int32_t(n_vocab())));
}
void model::reset() { llama_kv_cache_clear(_context.get()); }
void model::reset(uint32_t id) {
    if (!llama_kv_cache_seq_rm(_context.get(), llama_seq_id(id), -1, -1)) {
        throw std::runtime_error(ss::format("unable to reset seq_id: {}", id));
    }
}
batch::batch()
  : batch({0, 0, 0}) {}
batch::batch(config cfg)
  : _underlying(llama_batch_init(cfg.n_tokens, cfg.embd, cfg.n_seq_max))
  , _max_tokens(cfg.n_tokens) {}
batch::batch(batch&& other) noexcept
  : _underlying(std::exchange(other._underlying, {}))
  , _max_tokens(std::exchange(other._max_tokens, 0)) {}
batch& batch::operator=(batch&& other) noexcept {
    llama_batch_free(_underlying);
    _underlying = std::exchange(other._underlying, {});
    _max_tokens = std::exchange(other._max_tokens, 0);
    return *this;
}
batch::~batch() { llama_batch_free(_underlying); }

token token_vector::greedy_sample() const {
    auto max = std::max_element(
      _tokens.begin(),
      _tokens.end(),
      [](const llama_token_data& a, const llama_token_data& b) {
          return a.logit < b.logit;
      });
    return token(max->id);
}
void backend::initialize() {
    llama_backend_init();
    // attempt optimizations that help on some NUMA systems
    // - distribute: spread execution evenly over all nodes
    // - isolate: only spawn threads on CPUs on the node that execution
    //            started on
    // - numactl: use the CPU map provided my numactl
    llama_numa_init(GGML_NUMA_STRATEGY_ISOLATE);
    llama_log_set(
      [](ggml_log_level level, const char* msg, void* /*user_data*/) {
          ss::log_level lvl = ss::log_level::error;
          switch (level) {
          case GGML_LOG_LEVEL_ERROR:
              lvl = ss::log_level::error;
              break;
          case GGML_LOG_LEVEL_WARN:
              lvl = ss::log_level::warn;
              break;
          case GGML_LOG_LEVEL_INFO:
              lvl = ss::log_level::info;
              break;
          case GGML_LOG_LEVEL_DEBUG:
              lvl = ss::log_level::debug;
              break;
          }
          vlogl(logger, lvl, "{}", absl::StripAsciiWhitespace(msg));
      },
      /*user_data=*/nullptr);
}

void backend::shutdown() { llama_backend_free(); }
batch_view batch::subspan(position start, uint32_t size) const {
    return {*this, start, size};
}
// NOLINTBEGIN(*-pointer-arithmetic)
batch_view::batch_view(const batch& b, position start, uint32_t size)
  : _underlying({
    .n_tokens = int32_t(size),
    .token = b._underlying.token + start(),
    .embd = nullptr,
    .pos = b._underlying.pos + start(),
    .n_seq_id = b._underlying.n_seq_id + start(),
    .seq_id = b._underlying.seq_id + start(),
    .logits = b._underlying.logits + start(),
    // everything below can be ignored
    .all_pos_0 = b._underlying.all_pos_0,
    .all_pos_1 = b._underlying.all_pos_1,
    .all_seq_id = b._underlying.all_seq_id,
  }) {}
// NOLINTEND(*-pointer-arithmetic)
} // namespace ai::llama
