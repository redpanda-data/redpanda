/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "ai/service.h"

#include "base/vlog.h"
#include "llama.h"
#include "ssx/thread_worker.h"
#include "utils/named_type.h"

#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread_impl.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <absl/strings/ascii.h>

#include <ggml.h>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

namespace ai {

namespace {

// NOLINTNEXTLINE
static ss::logger ai_logger("ai");

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

namespace llama {

using large_language_model = handle<llama_model, llama_free_model>;
using context = handle<llama_context, llama_free>;
using token = named_type<llama_token, struct token_tag>;
using pos = named_type<llama_pos, struct pos_tag>;

large_language_model load_model(const std::filesystem::path& model_file) {
    llama_model_params model_params = llama_model_default_params();
    model_params.n_gpu_layers = std::numeric_limits<int32_t>::max();
    model_params.progress_callback_user_data = nullptr;
    model_params.progress_callback =
      [](float progress, void* user_data) -> bool {
        std::ignore = user_data;
        vlog(ai_logger.debug, "Loading model: {}%", progress * 100.0);
        // TODO: Return false to stop loading via abort source.
        return true;
    };
    model_params.use_mmap = true;   // How the model is loaded
    model_params.use_mlock = false; // Force the model to stay in RAM
    // TODO: Evaluate the rest of the parameters
    large_language_model model{
      llama_load_model_from_file(model_file.native().c_str(), model_params)};
    if (!model) {
        throw std::runtime_error(
          ss::format("unable to load model at: {}", model_file.native()));
    }
    return model;
}

context initialize_context(llama_model* model) {
    llama_context_params ctx_params = llama_context_default_params();

    ctx_params.seed = -1; // Use RNG
    ctx_params.n_ctx = 0; // Use the model's context window
    ctx_params.n_threads = std::thread::hardware_concurrency();
    ctx_params.n_threads_batch = ctx_params.n_threads; // use n_threads
    // TODO: Figure out the other parameters here.
    context ctx{llama_new_context_with_model(model, ctx_params)};
    if (!ctx) {
        throw std::runtime_error("unable to initialize model context");
    }
    return ctx;
}

std::vector<token>
tokenize(const llama_model* model, const ss::sstring& prompt) {
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
      model,
      prompt.data(),
      int32_t(prompt.size()),
      reinterpret_cast<llama_token*>(result.data()), // NOLINT
      int32_t(result.size()),
      add_bos,
      add_special);
    if (n_tokens < 0) {
        result.resize(-n_tokens);
        int32_t resized = llama_tokenize(
          model,
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

void append_decoded_token(
  const llama_model* model, token id, ss::sstring* output) {
    static constexpr size_t decoded_guess_size = 8;
    std::array<char, decoded_guess_size> decoded; // NOLINT
    int32_t result = llama_token_to_piece(
      model, id, decoded.data(), decoded.size());
    if (result >= 0) {
        output->append(decoded.data(), result);
        return;
    }
    ss::sstring decoded_str(ss::sstring::initialized_later(), -result);
    int32_t resized = llama_token_to_piece(
      model, id, decoded_str.data(), int32_t(decoded_str.size()));
    vassert(
      resized == -result,
      "expected string of length {} when decoding, but got {}",
      -result,
      resized);
    output->append(decoded_str.data(), decoded_str.size());
}

class batch {
public:
    batch(int32_t n_tokens, int32_t embd, int32_t n_seq_max)
      : _underlying(llama_batch_init(n_tokens, embd, n_seq_max))
      , _max_tokens(n_tokens) {}
    batch(const batch&) = delete;
    batch& operator=(const batch&) = delete;
    batch(batch&&) = delete;
    batch& operator=(batch&&) = delete;
    ~batch() { llama_batch_free(_underlying); }

    void add(
      token id,
      pos pos,
      const std::vector<llama_seq_id>& seq_ids,
      bool logits) {
#ifndef NDEBUG
        vassert(pos >= 0 && pos < _max_tokens, "out of bounds!");
#endif
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
    int32_t n_tokens() const { return _underlying.n_tokens; }
    void compute_logits(pos pos) {
        // NOLINTBEGIN(*-pointer-arithmetic)
        _underlying.logits[pos] = true;
        // NOLINTEND(*-pointer-arithmetic)
    }
    void clear() { _underlying.n_tokens = 0; }
    const llama_batch& raw() const { return _underlying; }

private:
    llama_batch _underlying;
    int32_t _max_tokens;
};

} // namespace llama
} // namespace

class model {
public:
    model(llama::large_language_model llm, llama::context ctx)
      : _llm(std::move(llm))
      , _ctx(std::move(ctx)) {}

    ss::sstring generate_text(
      const ss::sstring& prompt, service::generate_text_options opts) {
        llama_kv_cache_clear(_ctx.get());

        auto tokens = llama::tokenize(_llm.get(), prompt);
        auto n_ctx = int32_t(llama_n_ctx(_ctx.get()));
        const int32_t output_len = opts.max_tokens;
        int32_t n_kv_req = static_cast<int32_t>(tokens.size())
                           + (output_len - static_cast<int32_t>(tokens.size()));
        if (n_kv_req > n_ctx) {
            throw std::runtime_error(ss::format(
              "required KV cache size ({}) is not big enough, need at least {} "
              "tokens",
              n_ctx,
              n_kv_req));
        }
        constexpr size_t batch_n_tokens = 512;
        llama::batch batch(batch_n_tokens, /*embd=*/0, /*n_seq_max=*/1);

        // Add initial prompt
        std::vector seq_ids = {0};
        for (size_t i = 0; i < tokens.size(); ++i) {
            batch.add(
              tokens[i], llama::pos(int32_t(i)), seq_ids, /*logits=*/false);
        }
        // Output logits only for the last token of the prompt
        batch.compute_logits(llama::pos(batch.n_tokens() - 1));
        int32_t decode_result = llama_decode(_ctx.get(), batch.raw());
        if (decode_result != 0) {
            throw std::runtime_error(
              ss::format("failure to decode tokens: {}", decode_result));
        }

        int32_t n_cur = batch.n_tokens();

        auto n_vocab = llama_n_vocab(_llm.get());
        std::vector<llama_token_data> candidates;
        candidates.reserve(n_vocab);

        ss::sstring output;

        // The main loop to generate new tokens
        while (n_cur <= output_len) {
            std::span<float> logits(
              llama_get_logits_ith(_ctx.get(), batch.n_tokens() - 1), n_vocab);
            candidates.clear();
            for (llama_token token_id = 0; token_id < n_vocab; token_id++) {
                candidates.emplace_back(token_id, logits[token_id], 0.0f);
            }
            llama_token_data_array candidates_p = {
              .data = candidates.data(),
              .size = candidates.size(),
              .sorted = false,
            };
            auto new_token_id = llama::token(
              llama_sample_token_greedy(_ctx.get(), &candidates_p));
            if (
              new_token_id == llama_token_eos(_llm.get())
              || n_cur == output_len) {
                break;
            }

            llama::append_decoded_token(_llm.get(), new_token_id, &output);

            batch.clear();
            batch.add(
              new_token_id,
              llama::pos(n_cur),
              seq_ids,
              /*logits=*/true);
            ++n_cur;
            decode_result = llama_decode(_ctx.get(), batch.raw());
            if (decode_result != 0) {
                throw std::runtime_error(
                  ss::format("failure to decode tokens: {}", decode_result));
            }
        }

        return output;
    }

private:
    llama::large_language_model _llm;
    llama::context _ctx;
};

service::service() noexcept = default;
service::~service() noexcept = default;

constexpr ss::shard_id model_shard = 0;

ss::future<> service::start(config cfg) {
    if (ss::this_shard_id() != model_shard) {
        co_return;
    }
    _worker = std::make_unique<ssx::singleton_thread_worker>();
    co_await _worker->start({
      .name = "ai",
    });
    _model = co_await _worker->submit([&cfg] {
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
              vlogl(ai_logger, lvl, "{}", absl::StripAsciiWhitespace(msg));
          },
          /*user_data=*/nullptr);
        auto llm = llama::load_model(cfg.model_file);
        auto ctx = llama::initialize_context(llm.get());
        return std::make_unique<model>(std::move(llm), std::move(ctx));
    });
}

ss::future<> service::stop() {
    if (ss::this_shard_id() != model_shard) {
        co_return;
    }
    co_await _worker->submit([this] {
        _model = nullptr;
        llama_backend_free();
    });
    co_await _worker->stop();
}

ss::future<ss::sstring>
service::generate_text(ss::sstring prompt, generate_text_options opts) {
    return container().invoke_on(
      model_shard,
      [](service& s, ss::sstring prompt, generate_text_options opts) {
          return s.do_generate_text(std::move(prompt), opts);
      },
      std::move(prompt),
      opts);
}

ss::future<ss::sstring>
service::do_generate_text(ss::sstring prompt, generate_text_options opts) {
    return _worker->submit([this, prompt = std::move(prompt), opts] {
        return _model->generate_text(prompt, opts);
    });
}
} // namespace ai
