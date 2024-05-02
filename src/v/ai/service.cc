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
#include "config/configuration.h"
#include "http/client.h"
#include "metrics/metrics.h"
#include "net/tls.h"
#include "net/tls_certificate_probe.h"
#include "net/transport.h"
#include "net/types.h"
#include "utils/directory_walker.h"

#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread_impl.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/fixed_array.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <ada.h>
#include <atomic>
#include <exception>
#include <memory>
#include <stdexcept>
#include <variant>

namespace ai {

namespace {
// NOLINTNEXTLINE
static seastar::logger logger("ai");

struct model_parameters {
    llama::backend* backend;
    huggingface_file source;
    llama::model::config config;
};

struct generate_text_request {
    ss::sstring text;
    int32_t max_tokens;
};

struct generate_text_response {
    ss::sstring output;
};

struct compute_embeddings_request {
    ss::sstring text;
};

struct compute_embeddings_response {
    std::vector<float> embeddings;
};

} // namespace

class embeddings_model final
  : public ai::batched_thread_worker<
      model_parameters,
      compute_embeddings_request,
      compute_embeddings_response> {
    struct slot {
        int32_t id;
        llama::token sampled;
        llama::position pos;
        uint32_t i_batch = 0;
        enum class state { free, response };
        state state = state::free;
        work_item* item = nullptr;
    };

public:
    explicit embeddings_model(ss::sstring label, size_t max_parallel_requests)
      : _max_parallel_requests(max_parallel_requests) {
        namespace sm = ss::metrics;
        auto l = sm::label("model");
        _internal_metrics.add_group(
          "ai_service",
          {sm::make_counter(
             "embeddings_prompt_tokens_processed_count",
             sm::description("number of prompt tokens processed"),
             {l(label)},
             [this] { return prompt_tokens_processed(); })
             .aggregate({sm::shard_label})});
    }
    embeddings_model(const embeddings_model&) = delete;
    embeddings_model(embeddings_model&&) = delete;
    embeddings_model& operator=(const embeddings_model&) = delete;
    embeddings_model& operator=(embeddings_model&&) = delete;
    ~embeddings_model() final = default;

    ss::sstring description() const { return _llm->description(); }
    huggingface_file source() const { return _source; }
    int64_t prompt_tokens_processed() const { return _prompt_tokens; }

protected:
    void initialize(model_parameters p) noexcept final {
        vlog(logger.info, "initializing model at {}", p.config.model_file);
        _llm = std::make_unique<llama::model>(p.backend->load(p.config));
        _source = p.source;
        _batch = llama::batch({
          .n_tokens = _llm->n_batch(),
          .embd = 0,
          .n_seq_max = int32_t(_max_parallel_requests),
        });
    }
    void deinitialize() noexcept final {
        vlog(logger.info, "stopping model");
        _llm = nullptr;
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
            auto tokens = _llm->tokenize(param.text);
            if (tokens.empty() || tokens.back() != _llm->eos()) {
                tokens.push_back(_llm->eos());
            }
            _prompt_tokens += int64_t(tokens.size());
            slot = {
              .id = slot.id,
              .sampled = llama::token(-1),
              .pos = llama::position(0),
              .i_batch = 0, // initialized later
              .state = slot::state::response,
              .item = item,
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
                std::span<float> embeddings = _llm->get_embeddings(
                  slot.id, llama::position(int32_t(slot.i_batch) - i));
                std::vector<float> copy{embeddings.begin(), embeddings.end()};
                send_response(slot.item, {std::move(copy)});
                slot.state = slot::state::free;
                _llm->reset(slot.id);
            }
            vlog(logger.debug, "decoding batch complete");
        }
    }

    size_t _max_parallel_requests;
    llama::batch _batch;
    huggingface_file _source;
    std::unique_ptr<llama::model> _llm;
    std::atomic_int64_t _prompt_tokens;
    metrics::internal_metric_groups _internal_metrics;
};

class text_generation_model final
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
    explicit text_generation_model(
      ss::sstring label, size_t max_parallel_requests)
      : _max_parallel_requests(max_parallel_requests) {
        namespace sm = ss::metrics;
        auto l = sm::label("model");
        _internal_metrics.add_group(
          "ai_service",
          {
            sm::make_counter(
              "prompt_tokens_processed_count",
              sm::description("number of prompt tokens processed"),
              {l(label)},
              [this] { return prompt_tokens_processed(); })
              .aggregate({sm::shard_label}),
            sm::make_counter(
              "response_tokens_processed_count",
              sm::description("number of response tokens processed"),
              {l(label)},
              [this] { return response_tokens_processed(); })
              .aggregate({sm::shard_label}),
          });
    }
    text_generation_model(const text_generation_model&) = delete;
    text_generation_model(text_generation_model&&) = delete;
    text_generation_model& operator=(const text_generation_model&) = delete;
    text_generation_model& operator=(text_generation_model&&) = delete;
    ~text_generation_model() final = default;

    ss::sstring description() const { return _llm->description(); }
    huggingface_file source() const { return _source; }
    int64_t prompt_tokens_processed() const { return _prompt_tokens; }
    int64_t response_tokens_processed() const { return _response_tokens; }

protected:
    void initialize(model_parameters p) noexcept final {
        vlog(logger.info, "initializing model at {}", p.config.model_file);
        _llm = std::make_unique<llama::model>(p.backend->load(p.config));
        _source = p.source;
        _batch = llama::batch({
          .n_tokens = _llm->n_batch(),
          .embd = 0,
          .n_seq_max = int32_t(_max_parallel_requests),
        });
    }
    void deinitialize() noexcept final {
        vlog(logger.info, "stopping model");
        _llm = nullptr;
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
            auto tokens = _llm->tokenize(param.text);
            if (tokens.empty() || tokens.back() != _llm->eos()) {
                tokens.push_back(_llm->eos());
            }
            _prompt_tokens += int64_t(tokens.size());
            slot = {
              .id = slot.id,
              .sampled = llama::token(-1),
              .pos = llama::position(0),
              .i_batch = 0, // initialized later
              .state = slot::state::response,
              .item = item,
              .tokens_remaining = param.max_tokens,
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

        // Greedy sampling state
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
                ++_response_tokens;
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
    huggingface_file _source;
    std::unique_ptr<llama::model> _llm;
    std::atomic_int64_t _prompt_tokens;
    std::atomic_int64_t _response_tokens;
    metrics::internal_metric_groups _internal_metrics;
};

struct model {
    std::variant<
      std::unique_ptr<text_generation_model>,
      std::unique_ptr<embeddings_model>>
      kind;
};

service::service(config cfg) noexcept
  : _config(std::move(cfg)) {}
service::~service() noexcept = default;

constexpr ss::shard_id model_shard = 0;

ss::future<> service::start() {
    if (ss::this_shard_id() != model_shard) {
        co_return;
    }
    auto u = co_await _mu.hold_write_lock();
    // TODO: Create and init the backend on an alien thread to prevent stalls
    _backend = std::make_unique<llama::backend>();
    _backend->initialize();

    // Load the existing models
    auto embd_dir = std::string(_config.embeddings_path);
    co_await ss::recursive_touch_directory(embd_dir);
    co_await directory_walker::walk(
      embd_dir, [this](const ss::directory_entry& de) {
          if (
            de.type.value_or(ss::directory_entry_type::unknown)
            != ss::directory_entry_type::regular) {
              return ss::now();
          }
          if (de.name.ends_with(".tmp")) {
              return ss::remove_file(std::string(
                _config.embeddings_path / std::string_view(de.name)));
          }
          return load_embeddings_model(de.name);
      });

    auto llm_dir = std::string(_config.llm_path);
    co_await ss::recursive_touch_directory(llm_dir);
    co_await directory_walker::walk(
      llm_dir, [this](const ss::directory_entry& de) {
          if (
            de.type.value_or(ss::directory_entry_type::unknown)
            != ss::directory_entry_type::regular) {
              return ss::now();
          }
          if (de.name.ends_with(".tmp")) {
              return ss::remove_file(
                std::string(_config.llm_path / std::string_view(de.name)));
          }
          return load_llm(de.name);
      });
}

ss::future<> service::stop() {
    if (ss::this_shard_id() != model_shard) {
        co_return;
    }
    auto u = co_await _mu.hold_write_lock();
    for (auto& [_, model] : _models) {
        co_await ss::visit(model->kind, [](auto& m) { return m->stop(); });
    }
    // TODO: run this on another thread to prevent stalls
    _backend->shutdown();
}

namespace {

// A stream copy that keeps the input parameter alive.
ss::future<>
copy(ss::input_stream<char> input, ss::output_stream<char>* output) {
    co_await ss::copy(input, *output);
}

struct downloader {
    size_t redrs = 0;
    ss::shared_ptr<ss::tls::certificate_credentials> credentials;

    ss::future<>
    download_model_to_file(ss::sstring url, std::filesystem::path output) {
        vlog(
          logger.info, "downloading model from url {} to file {}", url, output);
        if (++redrs > 10) {
            throw std::runtime_error("proxy loop");
        }
        auto parsed = ada::parse(url);
        if (!parsed.has_value()) {
            throw std::runtime_error("invalid URL");
        }
        net::base_transport::configuration client_config;
        uint32_t port = parsed->scheme_default_port();
        if (parsed->has_port()) {
            if (!absl::SimpleAtoi(parsed->get_port().substr(1), &port)) {
                throw std::runtime_error(
                  ss::format("invalid port: {}", parsed->get_port()));
            }
        }
        client_config.server_addr = net::unresolved_address(
          ss::sstring(parsed->get_hostname()), uint16_t(port));
        client_config.disable_metrics = net::metrics_disabled::yes;
        client_config.disable_public_metrics
          = net::public_metrics_disabled::yes;
        if (parsed->get_protocol() == "https:") {
            if (!credentials) {
                ss::tls::credentials_builder builder;
                builder.set_client_auth(ss::tls::client_auth::NONE);
                auto ca_file = co_await net::find_ca_file();
                if (ca_file) {
                    co_await builder.set_x509_trust_file(
                      ca_file.value(), ss::tls::x509_crt_format::PEM);
                } else {
                    co_await builder.set_system_trust();
                }
                credentials
                  = co_await net::build_reloadable_credentials_with_probe<
                    ss::tls::certificate_credentials>(
                    std::move(builder), "ai_client", "httpclient");
            }
            client_config.credentials = credentials;
            client_config.tls_sni_hostname = ss::sstring(
              parsed->get_hostname());
        }
        http::client c(client_config);
        http::client::request_header header;
        header.method(boost::beast::http::verb::get);
        header.target(absl::StrCat(
          parsed->get_pathname(), parsed->get_search(), parsed->get_hash()));
        header.insert(
          boost::beast::http::field::host,
          {parsed->get_host().data(), parsed->get_host().length()});
        co_await ss::recursive_touch_directory(
          std::string(output.parent_path()));
        auto file = co_await ss::open_file_dma(
          std::string(output),
          ss::open_flags::rw | ss::open_flags::create
            | ss::open_flags::exclusive);
        auto output_stream = co_await ss::make_file_output_stream(
          std::move(file));
        auto redirect
          = co_await http::with_client(
              std::move(c),
              [req = std::move(header),
               &output_stream](http::client& client) mutable {
                  return client.request(std::move(req))
                    .then([&output_stream](
                            const http::client::response_stream_ref& res) {
                        return res->prefetch_headers().then([res,
                                                             &output_stream] {
                            auto it = res->get_headers().find(
                              boost::beast::http::field::location);
                            if (it == res->get_headers().end()) {
                                return copy(
                                         res->as_input_stream(), &output_stream)
                                  .then([]() -> std::optional<ss::sstring> {
                                      return std::nullopt;
                                  });
                            } else {
                                auto value = it->value();
                                return ssx::now(std::make_optional<ss::sstring>(
                                  value.data(), value.size()));
                            }
                        });
                    });
              })
              .finally([&output_stream] { return output_stream.close(); });
        // TODO: prevent infinite loops
        if (redirect) {
            co_await ss::remove_file(std::string(output));
            co_await download_model_to_file(*redirect, output);
        }
    }
};

} // namespace

ss::future<model_id> service::deploy_embeddings_model(huggingface_file f) {
    return container().invoke_on(
      model_shard, &service::do_deploy_embeddings_model, std::move(f));
}

ss::future<model_id> service::deploy_text_generation_model(huggingface_file f) {
    return container().invoke_on(
      model_shard, &service::do_deploy_text_generation_model, std::move(f));
}

ss::future<model_id>
service::do_deploy_embeddings_model(huggingface_file hg_file) {
    auto u = co_await _mu.hold_write_lock();
    auto uuid = uuid_t::create();
    auto tmp_out = _config.embeddings_path / fmt::format("{}.tmp", uuid);
    downloader d;
    co_await d.download_model_to_file(hg_file.to_url(), tmp_out);
    auto name = absl::StrJoin(
      {fmt::format("{}", uuid),
       absl::Base64Escape(hg_file.repo),
       absl::Base64Escape(hg_file.filename),
       std::string("gguf")},
      ".");
    auto final_out = _config.embeddings_path / name;
    co_await ss::rename_file(std::string(tmp_out), std::string(final_out));
    co_await ss::sync_directory(std::string(_config.embeddings_path));
    co_await load_embeddings_model(name);
    co_return model_id(uuid);
}

ss::future<model_id>
service::do_deploy_text_generation_model(huggingface_file hg_file) {
    auto u = co_await _mu.hold_write_lock();
    auto uuid = uuid_t::create();
    auto tmp_out = _config.llm_path / fmt::format("{}.tmp", uuid);
    downloader d;
    co_await d.download_model_to_file(hg_file.to_url(), tmp_out);
    auto name = absl::StrJoin(
      {fmt::format("{}", uuid),
       absl::Base64Escape(hg_file.repo),
       absl::Base64Escape(hg_file.filename),
       std::string("gguf")},
      ".");
    auto final_out = _config.llm_path / name;
    co_await ss::rename_file(std::string(tmp_out), std::string(final_out));
    co_await ss::sync_directory(std::string(_config.llm_path));
    co_await load_llm(name);
    co_return model_id(uuid);
}

ss::future<std::vector<model_info>> service::list_models() {
    return container().invoke_on(model_shard, &service::do_list_models);
}

ss::future<std::vector<model_info>> service::do_list_models() {
    auto u = co_await _mu.hold_read_lock();
    std::vector<model_info> models;
    for (const auto& [id, model] : _models) {
        auto desc = ss::visit(
          model->kind, [](auto& m) { return m->description(); });
        auto hf = ss::visit(model->kind, [](auto& m) { return m->source(); });
        models.emplace_back(id, model_name(std::move(desc)), std::move(hf));
    }
    co_return models;
}

ss::future<std::vector<float>>
service::compute_embeddings(model_id id, ss::sstring text) {
    if (text.empty()) {
        return ss::make_ready_future<std::vector<float>>();
    }
    return container().invoke_on(
      model_shard, &service::do_compute_embeddings, id, std::move(text));
}

ss::future<std::vector<float>>
service::do_compute_embeddings(model_id id, ss::sstring text) {
    auto u = co_await _mu.hold_read_lock();
    auto it = _models.find(id);
    if (it == _models.end()) {
        throw std::runtime_error("model does not exist");
    }
    auto response = co_await ss::visit(
      it->second->kind,
      [t = std::move(text)](std::unique_ptr<embeddings_model>& m) mutable {
          return m->submit({
            .text = std::move(t),
          });
      },
      [](const std::unique_ptr<text_generation_model>&) {
          return ss::make_exception_future<compute_embeddings_response>(
            std::runtime_error("wrong model kind"));
      });
    co_return std::move(response.embeddings);
}

ss::future<ss::sstring> service::generate_text(
  model_id id, ss::sstring prompt, generate_text_options opts) {
    if (prompt.empty()) {
        return ss::make_ready_future<ss::sstring>();
    }
    return container().invoke_on(
      model_shard, &service::do_generate_text, id, std::move(prompt), opts);
}

ss::future<ss::sstring> service::do_generate_text(
  model_id id, ss::sstring prompt, generate_text_options opts) {
    auto u = co_await _mu.hold_read_lock();
    auto it = _models.find(id);
    if (it == _models.end()) {
        throw std::runtime_error("model does not exist");
    }
    auto response = co_await ss::visit(
      it->second->kind,
      [p = std::move(prompt),
       opts](std::unique_ptr<text_generation_model>& m) mutable {
          return m->submit({
            .text = std::move(p),
            .max_tokens = opts.max_tokens,
          });
      },
      [](const std::unique_ptr<embeddings_model>&) {
          return ss::make_exception_future<generate_text_response>(
            std::runtime_error("wrong model kind"));
      });
    co_return std::move(response.output);
}

namespace {
std::tuple<model_id, std::string, std::string>
parse_filename(std::string_view disk_filename) {
    std::vector<std::string> parts = absl::StrSplit(disk_filename, '.');
    if (parts.size() != 4) {
        throw std::runtime_error("invalid file");
    }
    auto id = model_id(uuid_t::from_string(parts[0]));
    std::string repo;
    if (!absl::Base64Unescape(parts[1], &repo)) {
        throw std::runtime_error("invalid file");
    }
    std::string file;
    if (!absl::Base64Unescape(parts[2], &file)) {
        throw std::runtime_error("invalid file");
    }
    return std::make_tuple(id, repo, file);
}
} // namespace

ss::future<> service::load_embeddings_model(const ss::sstring& filename) {
    auto [id, repo, file] = parse_filename(filename);
    const auto& cluster_cfg = ::config::shard_local_cfg();
    // TODO(ai): All of these params should be per model, not cluster configs
    auto embdm = std::make_unique<embeddings_model>(
      fmt::format("{}/{}", repo, file),
      /*max_parallel_requests=*/cluster_cfg.parallel_requests);
    co_await embdm->start({
        .name = "ai-" + ss::sstring(id()).substr(0, 8),  // NOLINT
        .parameters = {
            .backend = _backend.get(),
            .source = {
              .repo = repo,
              .filename = file,
            },
            .config = {
               .model_file = _config.embeddings_path / std::string_view(filename),
               .nthreads = cluster_cfg.n_threads,
               .context_window = cluster_cfg.context_window,
               .max_sequences = cluster_cfg.max_seqs,
               .max_logical_batch_size = cluster_cfg.nbatch,
               .max_physical_batch_size = cluster_cfg.ubatch,
           },
        },
    });
    _models.emplace(id, std::make_unique<model>(std::move(embdm)));
    co_return;
}

ss::future<> service::load_llm(const ss::sstring& filename) {
    auto [id, repo, file] = parse_filename(filename);
    const auto& cluster_cfg = ::config::shard_local_cfg();
    // TODO(ai): All of these params should be per model, not cluster configs
    auto tgm = std::make_unique<text_generation_model>(
      fmt::format("{}/{}", repo, file),
      /*max_parallel_requests=*/cluster_cfg.parallel_requests);
    co_await tgm->start({
        .name = "ai-" + ss::sstring(id()).substr(0, 8),  // NOLINT
        .parameters = {
            .backend = _backend.get(),
            .source = {
              .repo = repo,
              .filename = file,
            },
            .config = {
               .model_file = _config.llm_path / std::string_view(filename),
               .nthreads = cluster_cfg.n_threads,
               .context_window = cluster_cfg.context_window,
               .max_sequences = cluster_cfg.max_seqs,
               .max_logical_batch_size = cluster_cfg.nbatch,
               .max_physical_batch_size = cluster_cfg.ubatch,
           },
        },
    });
    _models.emplace(id, std::make_unique<model>(std::move(tgm)));
}

ss::sstring huggingface_file::to_url() const {
    return ss::format(
      "https://huggingface.co/{}/resolve/main/{}", repo, filename);
}

} // namespace ai
