/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "wasm/wasmedge.h"

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "model/record.h"
#include "model/transform.h"
#include "reflection/type_traits.h"
#include "seastarx.h"
#include "storage/parser_utils.h"
#include "units.h"
#include "utils/mutex.h"
#include "utils/source_location.h"
#include "vlog.h"
#include "wasm/errc.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"
#include "wasm/probe.h"
#include "wasm/schema_registry.h"
#include "wasm/schema_registry_module.h"
#include "wasm/transform_module.h"
#include "wasm/wasi.h"
#include "wasm/work_queue.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/thread_cputime_clock.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/type_traits/function_traits.hpp>
#include <wasmedge/wasmedge.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace wasm::wasmedge {

namespace {
template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;
using WasmEdgeConfig
  = handle<WasmEdge_ConfigureContext, &WasmEdge_ConfigureDelete>;
using WasmEdgeStore = handle<WasmEdge_StoreContext, &WasmEdge_StoreDelete>;
using WasmEdgeVM = handle<WasmEdge_VMContext, &WasmEdge_VMDelete>;
using WasmEdgeLoader = handle<WasmEdge_LoaderContext, &WasmEdge_LoaderDelete>;
using WasmEdgeASTModule
  = handle<WasmEdge_ASTModuleContext, &WasmEdge_ASTModuleDelete>;
using WasmEdgeModule
  = handle<WasmEdge_ModuleInstanceContext, &WasmEdge_ModuleInstanceDelete>;
using WasmEdgeFuncType
  = handle<WasmEdge_FunctionTypeContext, &WasmEdge_FunctionTypeDelete>;
} // namespace

namespace {

class memory : public ffi::memory {
public:
    explicit memory(WasmEdge_MemoryInstanceContext* mem)
      : ffi::memory()
      , _underlying(mem) {}

    void* translate_raw(size_t guest_ptr, size_t len) final {
        void* ptr = WasmEdge_MemoryInstanceGetPointer(
          _underlying, guest_ptr, len);
        if (ptr == nullptr) [[unlikely]] {
            throw wasm_exception(
              ss::format(
                "Out of bounds memory access in FFI: {} + {} >= {} (pages)",
                guest_ptr,
                len,
                WasmEdge_MemoryInstanceGetPageSize(_underlying)),
              errc::user_code_failure);
        }
        return ptr;
    }

private:
    WasmEdge_MemoryInstanceContext* _underlying;
};

std::vector<WasmEdge_ValType>
convert_to_wasmedge(const std::vector<ffi::val_type>& ffi_types) {
    std::vector<WasmEdge_ValType> wasmedge_types;
    wasmedge_types.reserve(ffi_types.size());
    for (auto ty : ffi_types) {
        switch (ty) {
        case ffi::val_type::i32:
            wasmedge_types.push_back(WasmEdge_ValType_I32);
            break;
        case ffi::val_type::i64:
            wasmedge_types.push_back(WasmEdge_ValType_I64);
            break;
        }
    }
    return wasmedge_types;
}

template<typename T>
WasmEdge_Value convert_to_wasmedge(T value) {
    if constexpr (ss::is_future<T>::value) {
        return convert_to_wasmedge(value.get());
    } else if constexpr (reflection::is_rp_named_type<T>) {
        return convert_to_wasmedge(value());
    } else if constexpr (
      std::is_integral_v<T> && sizeof(T) == sizeof(int64_t)) {
        return WasmEdge_ValueGenI64(value);
    } else if constexpr (std::is_integral_v<T>) {
        return WasmEdge_ValueGenI32(value);
    } else {
        static_assert(
          ffi::detail::dependent_false<T>::value,
          "Unsupported wasm result type");
    }
}

// This allows for deducing the module and host function, which then using the
// function signature we generate the right types to register the function with
// WasmEdge.
template<auto value>
struct host_function;
template<
  typename Module,
  typename ReturnType,
  typename... ArgTypes,
  ReturnType (Module::*module_func)(ArgTypes...)>
struct host_function<module_func> {
    static void reg(
      const WasmEdgeModule& wasmedge_module,
      Module* host_module,
      std::string_view function_name) {
        std::vector<ffi::val_type> ffi_inputs;
        ffi::transform_types<ArgTypes...>(ffi_inputs);
        std::vector<ffi::val_type> ffi_outputs;
        ffi::transform_types<ReturnType>(ffi_outputs);
        auto inputs = convert_to_wasmedge(ffi_inputs);
        auto outputs = convert_to_wasmedge(ffi_outputs);
        auto func_type_ctx = WasmEdgeFuncType(WasmEdge_FunctionTypeCreate(
          inputs.data(), inputs.size(), outputs.data(), outputs.size()));

        if (!func_type_ctx) {
            vlog(
              wasm_log.warn,
              "Failed to register host function: {}",
              function_name);
            throw wasm::wasm_exception(
              ss::format("Unable to register {}", function_name),
              errc::engine_creation_failure);
        }

        WasmEdge_FunctionInstanceContext* func
          = WasmEdge_FunctionInstanceCreate(
            func_type_ctx.get(),
            [](
              void* data,
              const WasmEdge_CallingFrameContext* calling_ctx,
              const WasmEdge_Value* guest_params,
              WasmEdge_Value* guest_returns) {
                auto engine = static_cast<Module*>(data);
                auto mem = memory(
                  WasmEdge_CallingFrameGetMemoryInstance(calling_ctx, 0));
                std::vector<uint64_t> raw_params;
                size_t number_of_params = ffi::parameter_count<ArgTypes...>();
                raw_params.reserve(number_of_params);
                for (size_t i = 0; i < number_of_params; ++i) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    raw_params.push_back(WasmEdge_ValueGetI64(guest_params[i]));
                }
                auto host_params = ffi::extract_parameters<ArgTypes...>(
                  &mem, raw_params, 0);
                try {
                    if constexpr (std::is_void_v<ReturnType>) {
                        std::apply(
                          module_func,
                          std::tuple_cat(std::make_tuple(engine), host_params));
                    } else {
                        auto result = std::apply(
                          module_func,
                          std::tuple_cat(std::make_tuple(engine), host_params));
                        *guest_returns = convert_to_wasmedge(std::move(result));
                    }

                } catch (...) {
                    vlog(
                      wasm_log.warn,
                      "Error executing engine function: {}",
                      std::current_exception());
                    return WasmEdge_Result_Terminate;
                }
                return WasmEdge_Result_Success;
            },
            static_cast<void*>(host_module),
            /*cost=*/0);

        if (!func) {
            vlog(
              wasm_log.warn,
              "Failed to register host function: {}",
              function_name);
            throw wasm::wasm_exception(
              ss::format("Unable to register {}", function_name),
              errc::engine_creation_failure);
        }
        WasmEdge_ModuleInstanceAddFunction(
          wasmedge_module.get(),
          WasmEdge_StringWrap(function_name.data(), function_name.size()),
          func);
    }
};

void register_wasi_module(
  wasi::preview1_module* mod, const WasmEdgeModule& wasmedge_module) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&wasi::preview1_module::name>::reg(                          \
      wasmedge_module, mod, #name)
    REG_HOST_FN(args_get);
    REG_HOST_FN(args_sizes_get);
    REG_HOST_FN(environ_get);
    REG_HOST_FN(environ_sizes_get);
    REG_HOST_FN(clock_res_get);
    REG_HOST_FN(clock_time_get);
    REG_HOST_FN(fd_advise);
    REG_HOST_FN(fd_allocate);
    REG_HOST_FN(fd_close);
    REG_HOST_FN(fd_datasync);
    REG_HOST_FN(fd_fdstat_get);
    REG_HOST_FN(fd_fdstat_set_flags);
    REG_HOST_FN(fd_filestat_get);
    REG_HOST_FN(fd_filestat_set_size);
    REG_HOST_FN(fd_filestat_set_times);
    REG_HOST_FN(fd_pread);
    REG_HOST_FN(fd_prestat_get);
    REG_HOST_FN(fd_prestat_dir_name);
    REG_HOST_FN(fd_pwrite);
    REG_HOST_FN(fd_read);
    REG_HOST_FN(fd_readdir);
    REG_HOST_FN(fd_renumber);
    REG_HOST_FN(fd_seek);
    REG_HOST_FN(fd_sync);
    REG_HOST_FN(fd_tell);
    REG_HOST_FN(fd_write);
    REG_HOST_FN(path_create_directory);
    REG_HOST_FN(path_filestat_get);
    REG_HOST_FN(path_filestat_set_times);
    REG_HOST_FN(path_link);
    REG_HOST_FN(path_open);
    REG_HOST_FN(path_readlink);
    REG_HOST_FN(path_remove_directory);
    REG_HOST_FN(path_rename);
    REG_HOST_FN(path_symlink);
    REG_HOST_FN(path_unlink_file);
    REG_HOST_FN(poll_oneoff);
    REG_HOST_FN(proc_exit);
    REG_HOST_FN(sched_yield);
    REG_HOST_FN(random_get);
    REG_HOST_FN(sock_accept);
    REG_HOST_FN(sock_recv);
    REG_HOST_FN(sock_send);
    REG_HOST_FN(sock_shutdown);
#undef REG_HOST_FN
}

void register_transform_module(
  transform_module* mod, const WasmEdgeModule& wasmedge_module) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&transform_module::name>::reg(wasmedge_module, mod, #name)
    REG_HOST_FN(read_batch_header);
    REG_HOST_FN(read_record);
    REG_HOST_FN(write_record);
#undef REG_HOST_FN
}
void register_sr_module(
  schema_registry_module* mod, const WasmEdgeModule& wasmedge_module) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&schema_registry_module::name>::reg(                         \
      wasmedge_module, mod, #name)
    REG_HOST_FN(get_schema_definition);
    REG_HOST_FN(get_schema_definition_len);
    REG_HOST_FN(get_subject_schema);
    REG_HOST_FN(get_subject_schema_len);
    REG_HOST_FN(create_subject_schema);
#undef REG_HOST_FN
}

class wasmedge_engine final : public engine {
public:
    wasmedge_engine(
      std::string_view user_module_name,
      std::vector<WasmEdgeModule> modules,
      WasmEdgeStore s,
      WasmEdgeVM vm,
      std::unique_ptr<transform_module> transform_module,
      std::unique_ptr<schema_registry_module> sr_module,
      std::unique_ptr<wasi::preview1_module> wasi_module)
      : engine()
      , _modules(std::move(modules))
      , _store_ctx(std::move(s))
      , _vm_ctx(std::move(vm))
      , _user_module_name(user_module_name)
      , _rp_module(std::move(transform_module))
      , _sr_module(std::move(sr_module))
      , _wasi_module(std::move(wasi_module)) {}
    wasmedge_engine(const wasmedge_engine&) = delete;
    wasmedge_engine& operator=(const wasmedge_engine&) = delete;
    wasmedge_engine(wasmedge_engine&&) = delete;
    wasmedge_engine& operator=(wasmedge_engine&&) = delete;
    ~wasmedge_engine() final = default;

    std::string_view function_name() const final { return _user_module_name; }

    uint64_t memory_usage_size_bytes() const final {
        auto* mod = WasmEdge_VMGetActiveModule(_vm_ctx.get());
        if (!mod) {
            return 0;
        }
        uint32_t num_mems = WasmEdge_ModuleInstanceListMemoryLength(mod);
        std::vector<WasmEdge_String> memory_names;
        memory_names.assign(num_mems, {});
        uint32_t returned_mems = WasmEdge_ModuleInstanceListMemory(
          mod, memory_names.data(), memory_names.size());
        uint32_t num_pages = 0;
        for (uint32_t i = 0; i < returned_mems; ++i) {
            auto* mem = WasmEdge_ModuleInstanceFindMemory(mod, memory_names[i]);
            if (!mem) {
                continue;
            }
            num_pages += WasmEdge_MemoryInstanceGetPageSize(mem);
        }
        constexpr uint64_t page_size_bytes = 64_KiB;
        return num_pages * page_size_bytes;
    }

    ss::future<> start() final { return _queue.start(); }

    ss::future<> initialize() final {
        return _queue.enqueue<void>([this] { initialize_wasi(); });
    }

    ss::future<> stop() final { return _queue.stop(); }

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        vlog(wasm_log.trace, "Transforming batch: {}", batch.header());
        if (batch.compressed()) {
            model::record_batch decompressed
              = co_await storage::internal::decompress_batch(std::move(batch));
            if (decompressed.record_count() == 0) {
                co_return std::move(decompressed);
            }
            co_return co_await _queue.enqueue<model::record_batch>(
              [this, &decompressed, probe] {
                  return invoke_transform(&decompressed, probe);
              });
        } else {
            if (batch.record_count() == 0) {
                co_return batch;
            }
            co_return co_await _queue.enqueue<model::record_batch>(
              [this, &batch, probe] {
                  return invoke_transform(&batch, probe);
              });
        }
    }

private:
    void initialize_wasi() {
        vlog(
          wasm_log.debug, "Initializing wasm function {}", _user_module_name);

        std::array<WasmEdge_Value, 0> params = {};
        std::array<WasmEdge_Value, 0> returns = {};
        WasmEdge_Result result = WasmEdge_VMExecute(
          _vm_ctx.get(),
          WasmEdge_StringWrap(
            wasi::preview_1_start_function_name.data(),
            wasi::preview_1_start_function_name.size()),
          params.data(),
          params.size(),
          returns.data(),
          returns.size());
        if (!WasmEdge_ResultOK(result)) {
            vlog(
              wasm_log.warn,
              "Wasm function {} failed to init: {}",
              _user_module_name,
              WasmEdge_ResultGetMessage(result));
            // Get the right transform name here
            throw wasm_exception(
              ss::format(
                "wasi _start initialization {} failed: {}",
                _user_module_name,
                WasmEdge_ResultGetMessage(result)),
              errc::user_code_failure);
        }
        vlog(wasm_log.debug, "Wasm function {} initialized", _user_module_name);
    }

    model::record_batch
    invoke_transform(const model::record_batch* batch, transform_probe* probe) {
        return _rp_module->for_each_record(
          batch, [this, probe](wasm_call_params params) {
              _wasi_module->set_timestamp(params.current_record_timestamp);
              auto ml = probe->latency_measurement();
              WasmEdge_Result result;
              std::array args = {
                WasmEdge_ValueGenI32(params.batch_handle()),
                WasmEdge_ValueGenI32(params.record_handle()),
                WasmEdge_ValueGenI32(params.record_size),
                WasmEdge_ValueGenI32(params.current_record_offset)};
              std::array returns = {WasmEdge_ValueGenI32(-1)};
              try {
                  result = WasmEdge_VMExecute(
                    _vm_ctx.get(),
                    WasmEdge_StringWrap(
                      redpanda_on_record_callback_function_name.data(),
                      redpanda_on_record_callback_function_name.size()),
                    args.data(),
                    args.size(),
                    returns.data(),
                    returns.size());
              } catch (...) {
                  probe->transform_error();
                  vlog(
                    wasm_log.warn,
                    "transform failed! {}",
                    std::current_exception());
                  throw wasm_exception(
                    ss::format(
                      "transform execution {} failed: {}",
                      _user_module_name,
                      std::current_exception()),
                    errc::user_code_failure);
              }
              if (!WasmEdge_ResultOK(result)) {
                  probe->transform_error();
                  vlog(
                    wasm_log.warn,
                    "transform failed! {}",
                    WasmEdge_ResultGetMessage(result));
                  throw wasm_exception(
                    ss::format(
                      "transform execution {} failed", _user_module_name),
                    errc::user_code_failure);
              }
              auto user_result = WasmEdge_ValueGetI32(returns[0]);
              if (user_result != 0) {
                  probe->transform_error();
                  throw wasm_exception(
                    ss::format(
                      "transform execution {} resulted in error {}",
                      _user_module_name,
                      user_result),
                    errc::user_code_failure);
              }
          });
    }

    wasm::threaded_work_queue _queue;

    std::vector<WasmEdgeModule> _modules;
    WasmEdgeStore _store_ctx;
    WasmEdgeVM _vm_ctx;

    ss::sstring _user_module_name;
    std::unique_ptr<transform_module> _rp_module;
    std::unique_ptr<schema_registry_module> _sr_module;
    std::unique_ptr<wasi::preview1_module> _wasi_module;
};

WasmEdgeModule create_module(std::string_view name) {
    auto wrapped = WasmEdge_StringWrap(name.data(), name.size());
    return WasmEdgeModule(WasmEdge_ModuleInstanceCreate(wrapped));
}

class wasmedge_engine_factory : public factory {
public:
    wasmedge_engine_factory(
      WasmEdge_ConfigureContext* config_ctx,
      model::transform_metadata meta,
      iobuf wasm_module,
      schema_registry* sr,
      ss::logger* l)
      : _config_ctx(config_ctx)
      , _sr(sr)
      , _wasm_module(std::move(wasm_module))
      , _meta(std::move(meta))
      , _logger(l) {}

    ss::future<std::unique_ptr<engine>> make_engine() final {
        WasmEdge_Result result;

        auto store_ctx = WasmEdgeStore(WasmEdge_StoreCreate());

        auto vm_ctx = WasmEdgeVM(
          WasmEdge_VMCreate(_config_ctx, store_ctx.get()));

        // Register our module for transforms
        auto wasmedge_transform_module = create_module(transform_module::name);
        auto xform_module = std::make_unique<transform_module>();
        register_transform_module(
          xform_module.get(), wasmedge_transform_module);
        result = WasmEdge_VMRegisterModuleFromImport(
          vm_ctx.get(), wasmedge_transform_module.get());
        check_result(result, "failed to load module", errc::load_failure);

        // Register our module for schema registry
        auto wasmedge_sr_module = create_module(schema_registry_module::name);
        auto sr_module = std::make_unique<schema_registry_module>(_sr);
        register_sr_module(sr_module.get(), wasmedge_sr_module);
        result = WasmEdge_VMRegisterModuleFromImport(
          vm_ctx.get(), wasmedge_sr_module.get());
        check_result(result, "failed to load module", errc::load_failure);

        // Register our module stub wasi implementation
        auto wasmedge_wasi_module = create_module(wasi::preview1_module::name);
        std::vector<ss::sstring> args{_meta.name()};
        absl::flat_hash_map<ss::sstring, ss::sstring> env = _meta.environment;
        env.emplace("REDPANDA_INPUT_TOPIC", _meta.input_topic.tp());
        // NOTE: At the moment we validate that we only support a single output
        // topic, but in the future we may support multiple.
        env.emplace("REDPANDA_OUTPUT_TOPIC", _meta.output_topics.begin()->tp());
        auto wasi_module = std::make_unique<wasi::preview1_module>(
          args, env, _logger);
        register_wasi_module(wasi_module.get(), wasmedge_wasi_module);
        result = WasmEdge_VMRegisterModuleFromImport(
          vm_ctx.get(), wasmedge_wasi_module.get());
        check_result(result, "failed to load module", errc::load_failure);

        auto loader_ctx = WasmEdgeLoader(WasmEdge_LoaderCreate(_config_ctx));
        WasmEdge_ASTModuleContext* module_ctx_ptr = nullptr;
        // TODO(rockwood): This is a large allocation, we should not be doing
        // it.
        bytes b = iobuf_to_bytes(_wasm_module);
        result = WasmEdge_LoaderParseFromBuffer(
          loader_ctx.get(), &module_ctx_ptr, b.data(), b.size());
        auto module_ctx = WasmEdgeASTModule(module_ctx_ptr);
        check_result(result, "failed to load module", errc::load_failure);

        result = WasmEdge_VMLoadWasmFromASTModule(
          vm_ctx.get(), module_ctx.get());
        check_result(result, "failed to load module", errc::load_failure);

        result = WasmEdge_VMValidate(vm_ctx.get());
        check_result(
          result, "failed to create engine", errc::engine_creation_failure);

        result = WasmEdge_VMInstantiate(vm_ctx.get());
        check_result(
          result, "failed to create engine", errc::engine_creation_failure);
        std::vector<WasmEdgeModule> modules;
        modules.push_back(std::move(wasmedge_transform_module));
        modules.push_back(std::move(wasmedge_wasi_module));
        modules.push_back(std::move(wasmedge_sr_module));

        co_return std::make_unique<wasmedge_engine>(
          _meta.name(),
          std::move(modules),
          std::move(store_ctx),
          std::move(vm_ctx),
          std::move(xform_module),
          std::move(sr_module),
          std::move(wasi_module));
    }

private:
    void check_result(
      WasmEdge_Result result,
      std::string_view msg,
      errc ec,
      vlog::file_line fl = vlog::file_line::current()) {
        if (!WasmEdge_ResultOK(result)) {
            wasm_log.warn(
              "{} - {}: {}", fl, msg, WasmEdge_ResultGetMessage(result));
            throw wasm_exception(
              ss::format("{}: {}", msg, WasmEdge_ResultGetMessage(result)), ec);
        }
    }

    WasmEdge_ConfigureContext* _config_ctx;
    schema_registry* _sr;

    iobuf _wasm_module;
    model::transform_metadata _meta;
    ss::logger* _logger;
};

class wasmedge_runtime : public runtime {
public:
    explicit wasmedge_runtime(std::unique_ptr<schema_registry> sr)
      : _config_ctx(WasmEdge_ConfigureCreate())
      , _sr(std::move(sr)) {}

    ss::future<std::unique_ptr<factory>> make_factory(
      model::transform_metadata meta, iobuf buf, ss::logger* logger) final {
        // TODO: Move compilation to here and reuse the compiled artifact
        co_return std::make_unique<wasmedge_engine_factory>(
          _config_ctx.get(),
          std::move(meta),
          std::move(buf),
          _sr.get(),
          logger);
    }

private:
    WasmEdgeConfig _config_ctx;
    std::unique_ptr<schema_registry> _sr;
};

} // namespace

std::unique_ptr<runtime> create_runtime(std::unique_ptr<schema_registry> sr) {
    return std::make_unique<wasmedge_runtime>(std::move(sr));
}

} // namespace wasm::wasmedge
