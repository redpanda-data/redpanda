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

#include "ai/batched_thread_worker.h"
#include "ai/llama_bindings.h"
#include "base/vassert.h"
#include "base/vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread_impl.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <absl/container/fixed_array.h>

#include <exception>
#include <memory>
#include <stdexcept>
#include <vector>

namespace ai {

namespace {
// NOLINTNEXTLINE
static seastar::logger logger("ai");

struct model_parameters {
    std::filesystem::path model_file;
};

struct generate_text_request {
    ss::sstring prompt;
    service::generate_text_options options;
};

struct generate_text_response {
    ss::sstring text;
};

} // namespace

class model final
  : public ai::batched_thread_worker<
      model_parameters,
      generate_text_request,
      generate_text_response> {
    struct slot {
        int32_t id;
        llama::token sampled;
        llama::position pos;
        uint32_t i_batch = 0;
        enum class state { free, response };
        state state = state::free;
        work_item* item = nullptr;
        int32_t tokens_remaining;
        ss::sstring output;
    };

public:
    explicit model(size_t max_parallel_requests)
      : _max_parallel_requests(max_parallel_requests) {}
    model(const model&) = delete;
    model(model&&) = delete;
    model& operator=(const model&) = delete;
    model& operator=(model&&) = delete;
    ~model() final = default;

protected:
    void initialize(model_parameters p) noexcept final {
        vlog(logger.info, "initializing model at {}", p.model_file);
        _backend.initialize();
        _llm = std::make_unique<llama::model>(_backend.load(p.model_file));
        _batch = llama::batch({
          .n_tokens = _llm->n_batch(),
          .embd = 0,
          .n_seq_max = int32_t(_max_parallel_requests),
        });
    }
    void deinitialize() noexcept final {
        vlog(logger.info, "stopping model");
        _llm = nullptr;
        _backend.shutdown();
    }
    void process() noexcept final {
        _llm->reset();
        vlog(logger.info, "starting processing loop");

        absl::FixedArray<slot> slots(_max_parallel_requests);
        // initialize the ID for the slots
        for (int32_t i = 0; auto& s : slots) {
            s.id = i++;
        }
        try {
            while (true) {
                // TODO: Do we need to shift context if the context window is
                // running out?
                _batch.clear();
                batch_pending_responses(&slots);
                pickup_more_work(&slots);
                if (_batch.size() == 0) {
                    vlog(logger.info, "no work in batch, stopping");
                    break;
                }
                process_batch(&slots);
            }
        } catch (...) {
            vassert(false, "failed to process batch", std::current_exception());
        }
    }

private:
    /**
     * If we are currently processing the responses of prompts, continue to do
     * so until we run out of work.
     */
    void batch_pending_responses(absl::FixedArray<slot>* slots) {
        for (auto& s : *slots) {
            if (s.state == slot::state::free) {
                continue;
            }
            // if (s.state == slot::state::prompt) {
            //     // TODO: Handle partially evaluated prompts due to batch
            //     size. continue;
            // }
            s.i_batch = _batch.size();
            std::array seq_ids = {s.id};
            vlog(
              logger.trace,
              "continuing processing slot.id={} at position={} batch_idx={} "
              "remaining_tokens={}",
              s.id,
              s.pos,
              s.i_batch,
              s.tokens_remaining);
            _batch.add(s.sampled, s.pos++, seq_ids, /*logits=*/true);
        }
    }

    /**
     * If there is room left in the batch pick up new requests to add to the
     * batch.
     */
    void pickup_more_work(absl::FixedArray<slot>* slots) {
        for (auto& slot : *slots) {
            if (slot.state != slot::state::free) {
                continue;
            }
            work_item* item = pop();
            if (item == nullptr) {
                return;
            }
            auto param = item->take_parameter();
            auto tokens = _llm->tokenize(param.prompt);
            slot = {
              .id = slot.id,
              .sampled = llama::token(-1),
              .pos = llama::position(0),
              .i_batch = 0, // initialized later
              .state = slot::state::response,
              .item = item,
              .tokens_remaining = param.options.max_tokens,
              .output = "",
            };
            // TODO: Truncate large prompts
            for (llama::token token_id : tokens) {
                std::array seq_ids = {slot.id};
                _batch.add(token_id, slot.pos++, seq_ids, /*logits=*/false);
            }
            _batch.compute_logits(llama::position(_batch.size() - 1));
            slot.i_batch = _batch.size() - 1;
            vlog(
              logger.trace,
              "starting processing slot.id={} at prompt_tokens={} batch_idx={}",
              slot.id,
              slot.pos,
              slot.i_batch);
            if (_batch.size() >= _llm->n_batch()) {
                break;
            }
        }
    }

    /**
     * Process our batch by submitting it to the GPU
     */
    void process_batch(absl::FixedArray<slot>* slots) {
        vlog(logger.trace, "starting processing batch");
        auto max_batch_size = _llm->n_batch();

        // Greed sampling state
        auto n_vocab = _llm->n_vocab();
        llama::token_vector candidates;
        candidates.reserve(n_vocab);

        for (int32_t i = 0; i < _batch.size(); i += max_batch_size) {
            int32_t n_tokens = std::min(max_batch_size, _batch.size() - i);
            auto view = _batch.subspan(llama::position(i), n_tokens);
            if (!_llm->decode(view)) {
                if (max_batch_size == 1) {
                    throw std::runtime_error("unable to decode batch");
                }
                vlog(
                  logger.warn,
                  "failed to decode batch at size {}, trying with smaller "
                  "batch",
                  max_batch_size);
                // Retry with half of the batch size.
                max_batch_size /= 2;
                i -= max_batch_size;
                continue;
            }
            vlog(
              logger.trace,
              "successfully decoded batch [{}, {}]",
              i,
              i + n_tokens);
            for (auto& slot : *slots) {
                if (slot.state == slot::state::free) {
                    continue;
                }
                if (
                  slot.i_batch < uint32_t(i)
                  || slot.i_batch >= uint32_t(i + n_tokens)) {
                    continue;
                }
                std::span<float> logits = _llm->get_logits(
                  llama::position(int32_t(slot.i_batch) - i));

                candidates.clear();
                for (llama::token token_id : _llm->vocab()) {
                    candidates.add(token_id, logits[token_id], 0.0f);
                }
                auto new_token_id = candidates.greedy_sample();
                if (new_token_id == _llm->eos() || slot.tokens_remaining <= 0) {
                    send_response(slot.item, {std::move(slot.output)});
                    slot.state = slot::state::free;
                    _llm->reset(slot.id);
                    continue;
                }
                --slot.tokens_remaining;
                slot.sampled = new_token_id;
                _llm->append_decoded_token(new_token_id, &slot.output);
            }
            vlog(logger.debug, "decoding batch complete");
        }
    }

    size_t _max_parallel_requests;
    llama::batch _batch;
    llama::backend _backend;
    std::unique_ptr<llama::model> _llm;
};

service::service() noexcept = default;
service::~service() noexcept = default;

constexpr ss::shard_id model_shard = 0;

ss::future<> service::start(config cfg) {
    if (ss::this_shard_id() != model_shard) {
        co_return;
    }
    vlog(logger.info, "starting AI model...");
    _model = std::make_unique<model>(/*max_parallel_requests=*/8);
    _model->start({
         .name = "ai",
         .parameters = {
             .model_file = std::move(cfg.model_file),
         },
     });
}

ss::future<> service::stop() {
    if (ss::this_shard_id() != model_shard) {
        co_return;
    }
    co_await _model->stop();
}

ss::future<ss::sstring>
service::generate_text(ss::sstring prompt, generate_text_options opts) {
    if (prompt.empty()) {
        return ss::make_ready_future<ss::sstring>();
    }
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
    auto resp = co_await _model->submit(
      {.prompt = std::move(prompt), .options = opts});
    co_return std::move(resp.text);
}
} // namespace ai
