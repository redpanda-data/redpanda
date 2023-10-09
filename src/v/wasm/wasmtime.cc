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
#include "wasm/wasmtime.h"

#include "model/record.h"
#include "model/transform.h"
#include "ssx/thread_worker.h"
#include "storage/parser_utils.h"
#include "utils/type_traits.h"
#include "wasm/api.h"
#include "wasm/errc.h"
#include "wasm/ffi.h"
#include "wasm/logger.h"
#include "wasm/probe.h"
#include "wasm/schema_registry_module.h"
#include "wasm/transform_module.h"
#include "wasm/wasi.h"

#include <seastar/core/future.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <csignal>
#include <memory>
#include <pthread.h>
#include <span>
#include <wasm.h>
#include <wasmtime.h>

namespace wasm::wasmtime {

namespace {

constexpr uint64_t wasmtime_fuel_amount = 1'000'000'000;

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

void check_error(const wasmtime_error_t* error) {
    if (!error) {
        return;
    }
    wasm_name_t msg;
    wasmtime_error_message(error, &msg);
    std::string str(msg.data, msg.size);
    wasm_byte_vec_delete(&msg);
    throw wasm_exception(std::move(str), errc::load_failure);
}
void check_trap(const wasm_trap_t* trap) {
    if (!trap) {
        return;
    }
    wasm_name_t msg{.size = 0, .data = nullptr};
    wasm_trap_message(trap, &msg);
    std::stringstream sb;
    sb << std::string_view(msg.data, msg.size);
    wasm_byte_vec_delete(&msg);
    wasm_frame_vec_t trace{.size = 0, .data = nullptr};
    wasm_trap_trace(trap, &trace);
    for (wasm_frame_t* frame : std::span(trace.data, trace.size)) {
        auto* module_name = wasmtime_frame_module_name(frame);
        auto* function_name = wasmtime_frame_func_name(frame);
        if (module_name && function_name) {
            sb << std::endl
               << std::string_view(module_name->data, module_name->size) << "::"
               << std::string_view(function_name->data, function_name->size);
        } else if (module_name) {
            sb << std::endl
               << std::string_view(module_name->data, module_name->size)
               << "::??";
        } else if (function_name) {
            sb << std::endl
               << std::string_view(function_name->data, function_name->size);
        }
    }
    wasm_frame_vec_delete(&trace);
    throw wasm_exception(sb.str(), errc::user_code_failure);
}

std::vector<uint64_t> to_raw_values(std::span<const wasmtime_val_t> values) {
    std::vector<uint64_t> raw;
    raw.reserve(values.size());
    for (const wasmtime_val_t val : values) {
        switch (val.kind) {
        case WASMTIME_I32:
            raw.push_back(val.of.i32);
            break;
        case WASMTIME_I64:
            raw.push_back(val.of.i64);
            break;
        default:
            throw wasm_exception(
              ss::format("Unsupported value type: {}", val.kind),
              errc::user_code_failure);
        }
    }
    return raw;
}

wasm_valtype_vec_t
convert_to_wasmtime(const std::vector<ffi::val_type>& ffi_types) {
    wasm_valtype_vec_t wasm_types;
    wasm_valtype_vec_new_uninitialized(&wasm_types, ffi_types.size());
    std::span<wasm_valtype_t*> wasm_types_span{
      wasm_types.data, ffi_types.size()};
    for (size_t i = 0; i < ffi_types.size(); ++i) {
        switch (ffi_types[i]) {
        case ffi::val_type::i32:
            wasm_types_span[i] = wasm_valtype_new(WASM_I32);
            break;
        case ffi::val_type::i64:
            wasm_types_span[i] = wasm_valtype_new(WASM_I64);
            break;
        }
    }
    return wasm_types;
}
template<typename T>
wasmtime_val_t convert_to_wasmtime(T value) {
    if constexpr (reflection::is_rp_named_type<T>) {
        return convert_to_wasmtime(value());
    } else if constexpr (
      std::is_integral_v<T> && sizeof(T) == sizeof(int64_t)) {
        return wasmtime_val_t{
          .kind = WASMTIME_I64, .of = {.i64 = static_cast<int64_t>(value)}};
    } else if constexpr (std::is_integral_v<T>) {
        return wasmtime_val_t{
          .kind = WASMTIME_I32, .of = {.i32 = static_cast<int32_t>(value)}};
    } else {
        static_assert(
          utils::unsupported_type<T>::value, "Unsupported wasm result type");
    }
}

class memory : public ffi::memory {
public:
    explicit memory(wasmtime_context_t* ctx)
      : _ctx(ctx)
      , _underlying() {}

    void* translate_raw(size_t guest_ptr, size_t len) final {
        auto memory_size = wasmtime_memory_data_size(_ctx, &_underlying);
        if ((guest_ptr + len) > memory_size) [[unlikely]] {
            throw wasm_exception(
              ss::format(
                "Out of bounds memory access in FFI: {} + {} >= {}",
                guest_ptr,
                len,
                memory_size),
              errc::user_code_failure);
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return wasmtime_memory_data(_ctx, &_underlying) + guest_ptr;
    }

    void set_underlying_memory(wasmtime_memory_t* m) { _underlying = *m; }

private:
    wasmtime_context_t* _ctx;
    wasmtime_memory_t _underlying;
};

/**
 * Preinitialized instances only need a store to be plugged in and start
 * running. All compilation including any trampolines for host functions have
 * already been compiled in and no other code generation/compilation needs to
 * happen.
 *
 * Creating instances from this is as fast as allocating the memory needed and
 * running any startup functions for the module.
 */
struct preinitialized_instance {
    handle<wasmtime_instance_pre_t, wasmtime_instance_pre_delete> underlying
      = nullptr;
};

absl::flat_hash_map<ss::sstring, ss::sstring>
make_environment_vars(const model::transform_metadata& meta) {
    absl::flat_hash_map<ss::sstring, ss::sstring> env = meta.environment;
    env.emplace("REDPANDA_INPUT_TOPIC", meta.input_topic.tp());
    env.emplace("REDPANDA_OUTPUT_TOPIC", meta.output_topics.begin()->tp());
    return env;
}

class wasmtime_engine : public engine {
public:
    wasmtime_engine(
      wasm_engine_t* engine,
      model::transform_metadata metadata,
      ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>>
        preinitialized,
      schema_registry* sr,
      ss::logger* logger)
      : _engine(engine)
      , _meta(std::move(metadata))
      , _preinitialized(std::move(preinitialized))
      , _sr_module(sr)
      , _wasi_module({_meta.name()}, make_environment_vars(_meta), logger) {}
    wasmtime_engine(const wasmtime_engine&) = delete;
    wasmtime_engine& operator=(const wasmtime_engine&) = delete;
    wasmtime_engine(wasmtime_engine&&) = default;
    wasmtime_engine& operator=(wasmtime_engine&&) = default;
    ~wasmtime_engine() override = default;

    uint64_t memory_usage_size_bytes() const final {
        if (!_store) {
            return 0;
        }
        std::string_view memory_export_name = "memory";
        auto* ctx = wasmtime_store_context(_store.get());
        wasmtime_extern_t memory_extern;
        bool ok = wasmtime_instance_export_get(
          ctx,
          &_instance,
          memory_export_name.data(),
          memory_export_name.size(),
          &memory_extern);
        if (!ok || memory_extern.kind != WASMTIME_EXTERN_MEMORY) {
            return 0;
        }
        return wasmtime_memory_data_size(ctx, &memory_extern.of.memory);
    };

    ss::future<> start() final {
        co_await create_instance();
        co_await initialize_wasi();
    }

    ss::future<> stop() final {
        // Deleting the store invalidates the instance and actually frees the
        // memory for the underlying instance.
        _store = nullptr;
        vassert(
          !_pending_host_function,
          "pending host functions should be awaited upon before stopping the "
          "engine");
        co_return;
    }

    ss::future<model::record_batch>
    transform(model::record_batch batch, transform_probe* probe) override {
        vlog(wasm_log.trace, "Transforming batch: {}", batch.header());
        if (batch.record_count() == 0) {
            co_return std::move(batch);
        }
        if (batch.compressed()) {
            batch = co_await storage::internal::decompress_batch(
              std::move(batch));
        }
        ss::future<model::record_batch> fut = co_await ss::coroutine::as_future(
          invoke_transform(&batch, probe));
        if (fut.failed()) {
            probe->transform_error();
            std::rethrow_exception(fut.get_exception());
        }
        co_return std::move(fut).get();
    }

    template<typename T>
    T* get_module() noexcept {
        if constexpr (std::is_same_v<T, wasi::preview1_module>) {
            return &_wasi_module;
        } else if constexpr (std::is_same_v<T, transform_module>) {
            return &_transform_module;
        } else if constexpr (std::is_same_v<T, schema_registry_module>) {
            return &_sr_module;
        } else {
            static_assert(
              utils::unsupported_type<T>::value, "unsupported module");
        }
    }

    // Register that a pending async host function is happening, this future
    // must never fail.
    void register_pending_host_function(ss::future<> fut) noexcept {
        _pending_host_function.emplace(std::move(fut));
    }

private:
    void reset_fuel(wasmtime_context_t* ctx) {
        uint64_t fuel = 0;
        wasmtime_context_fuel_remaining(ctx, &fuel);
        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_context_add_fuel(ctx, wasmtime_fuel_amount - fuel));
    }

    ss::future<> create_instance() {
        // The underlying "data" for this store is our engine, which is what
        // allows our host functions to access the actual module for that host
        // function.
        handle<wasmtime_store_t, wasmtime_store_delete> store{
          wasmtime_store_new(_engine, /*data=*/this, /*finalizer=*/nullptr)};
        auto* context = wasmtime_store_context(store.get());
        reset_fuel(context);

        _wasi_module.set_timestamp(model::timestamp::min());

        // The wasm spec has a feature that a module can specify a startup
        // function that is run on start.
        wasm_trap_t* trap_ptr = nullptr;
        wasmtime_error_t* error_ptr = nullptr;
        handle<wasmtime_call_future_t, wasmtime_call_future_delete> fut{
          wasmtime_instance_pre_instantiate_async(
            _preinitialized->underlying.get(),
            context,
            &_instance,
            &trap_ptr,
            &error_ptr)};

        // Poll the call future to completion, yielding to the scheduler when
        // the future yields.
        while (!wasmtime_call_future_poll(fut.get())) {
            if (_pending_host_function) {
                auto host_future = std::exchange(_pending_host_function, {});
                co_await std::move(host_future).value();
                continue;
            }
            co_await ss::coroutine::maybe_yield();
        }

        // Now that the call future has returned as completed, we can assume the
        // out pointers have been set, we need to check them for errors.
        handle<wasmtime_error_t, wasmtime_error_delete> error{error_ptr};
        handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
        check_error(error.get());
        check_trap(trap.get());
        // initialization success (_instance is valid to use with this store)
        _store = std::move(store);
    }

    ss::future<> call_host_func(
      wasmtime_context_t* ctx,
      const wasmtime_func_t* func,
      std::span<const wasmtime_val_t> args,
      std::span<wasmtime_val_t> results) {
        reset_fuel(ctx);
        wasm_trap_t* trap_ptr = nullptr;
        wasmtime_error_t* err_ptr = nullptr;
        handle<wasmtime_call_future_t, wasmtime_call_future_delete> fut{
          wasmtime_func_call_async(
            ctx,
            func,
            args.data(),
            args.size(),
            results.data(),
            results.size(),
            &trap_ptr,
            &err_ptr)};

        // Poll the call future to completion, yielding to the scheduler when
        // the future yields.
        while (!wasmtime_call_future_poll(fut.get())) {
            if (_pending_host_function) {
                auto host_future = std::exchange(_pending_host_function, {});
                co_await std::move(host_future).value();
                continue;
            }
            co_await ss::coroutine::maybe_yield();
        }

        // Now that the call future has returned as completed, we can assume the
        // out pointers have been set, we need to check them for errors.
        // Any results have been written to results and it's on the caller to
        // check.
        handle<wasmtime_error_t, wasmtime_error_delete> error{err_ptr};
        handle<wasm_trap_t, wasm_trap_delete> trap{trap_ptr};
        check_error(error.get());
        check_trap(trap.get());
    }

    ss::future<> initialize_wasi() {
        auto* ctx = wasmtime_store_context(_store.get());
        wasmtime_extern_t start;
        bool ok = wasmtime_instance_export_get(
          ctx,
          &_instance,
          wasi::preview_1_start_function_name.data(),
          wasi::preview_1_start_function_name.size(),
          &start);
        if (!ok || start.kind != WASMTIME_EXTERN_FUNC) {
            throw wasm_exception(
              "Missing wasi initialization function", errc::user_code_failure);
        }
        vlog(wasm_log.info, "Initializing wasm function {}", _meta.name());
        co_await call_host_func(ctx, &start.of.func, {}, {});
        vlog(wasm_log.info, "Wasm function {} initialized", _meta.name());
    }

    ss::future<model::record_batch>
    invoke_transform(const model::record_batch* batch, transform_probe* p) {
        auto* ctx = wasmtime_store_context(_store.get());
        wasmtime_extern_t cb;
        bool ok = wasmtime_instance_export_get(
          ctx,
          &_instance,
          redpanda_on_record_callback_function_name.data(),
          redpanda_on_record_callback_function_name.size(),
          &cb);
        if (!ok || cb.kind != WASMTIME_EXTERN_FUNC) {
            throw wasm_exception(
              "Missing wasi initialization function", errc::user_code_failure);
        }
        wasmtime_val_t result;
        std::array<wasmtime_val_t, 4> args{};
        co_return co_await _transform_module.for_each_record_async(
          batch,
          [this, &ctx, &cb, &p, &result, &args](wasm_call_params params) {
              reset_fuel(ctx);
              _wasi_module.set_timestamp(params.current_record_timestamp);
              auto recorder = p->latency_measurement();
              args[0] = {
                .kind = WASMTIME_I32,
                .of = {.i32 = params.batch_handle},
              };
              args[1] = {
                .kind = WASMTIME_I32,
                .of = {.i32 = params.record_handle},
              };
              args[2] = {
                .kind = WASMTIME_I32,
                .of = {.i32 = params.record_size},
              };
              args[3] = {
                .kind = WASMTIME_I32,
                .of = {.i32 = params.current_record_offset},
              };
              result = {
                .kind = WASMTIME_I32,
                .of = {.i32 = -1},
              };
              return call_host_func(ctx, &cb.of.func, args, {&result, 1})
                .then([this, &result, r = std::move(recorder)] {
                    if (result.kind != WASMTIME_I32 || result.of.i32 != 0) {
                        throw wasm_exception(
                          ss::format(
                            "transform execution {} resulted in error {}",
                            _meta.name(),
                            result.of.i32),
                          errc::user_code_failure);
                    }
                });
          });
    }

    wasm_engine_t* _engine;
    model::transform_metadata _meta;
    ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>> _preinitialized;

    transform_module _transform_module;
    schema_registry_module _sr_module;
    wasi::preview1_module _wasi_module;

    // The following state is only valid if there is a non-null store.
    handle<wasmtime_store_t, wasmtime_store_delete> _store;
    wasmtime_instance_t _instance{};
    std::optional<ss::future<>> _pending_host_function;
};

template<auto value>
struct host_function;

template<
  typename Module,
  typename ReturnType,
  typename... ArgTypes,
  ReturnType (Module::*module_func)(ArgTypes...)>
struct host_function<module_func> {
public:
    /**
     * Register a host function such that it can be invoked from the Wasmtime
     * VM.
     */
    static void reg(wasmtime_linker_t* linker, std::string_view function_name) {
        std::vector<ffi::val_type> ffi_inputs;
        ffi::transform_types<ArgTypes...>(ffi_inputs);
        std::vector<ffi::val_type> ffi_outputs;
        ffi::transform_types<ReturnType>(ffi_outputs);
        auto inputs = convert_to_wasmtime(ffi_inputs);
        auto outputs = convert_to_wasmtime(ffi_outputs);
        // Takes ownership of inputs and outputs
        handle<wasm_functype_t, wasm_functype_delete> functype{
          wasm_functype_new(&inputs, &outputs)};

        if constexpr (ss::is_future<ReturnType>::value) {
            handle<wasmtime_error_t, wasmtime_error_delete> error(
              wasmtime_linker_define_async_func(
                linker,
                Module::name.data(),
                Module::name.size(),
                function_name.data(),
                function_name.size(),
                functype.get(),
                &invoke_async_host_fn,
                /*data=*/nullptr,
                /*finalizer=*/nullptr));
            check_error(error.get());
        } else {
            handle<wasmtime_error_t, wasmtime_error_delete> error(
              wasmtime_linker_define_func(
                linker,
                Module::name.data(),
                Module::name.size(),
                function_name.data(),
                function_name.size(),
                functype.get(),
                &invoke_sync_host_fn,
                /*data=*/nullptr,
                /*finalizer=*/nullptr));
            check_error(error.get());
        }
    }

private:
    /**
     * All the boilerplate setup needed to invoke a host function from the VM.
     *
     * Extracts the memory module, and handles exceptions from our host
     * functions.
     */
    static wasm_trap_t* invoke_sync_host_fn(
      void*,
      wasmtime_caller_t* caller,
      const wasmtime_val_t* args,
      size_t nargs,
      wasmtime_val_t* results,
      size_t nresults) {
        auto* context = wasmtime_caller_context(caller);
        auto* engine = static_cast<wasmtime_engine*>(
          wasmtime_context_get_data(context));
        auto* host_module = engine->get_module<Module>();
        memory mem(context);
        wasm_trap_t* trap = extract_memory(caller, &mem);
        if (trap != nullptr) {
            return trap;
        }
        try {
            do_invoke_sync_host_fn(
              host_module, &mem, {args, nargs}, {results, nresults});
            return nullptr;
        } catch (const std::exception& e) {
            vlog(wasm_log.warn, "Failure executing host function: {}", e);
            std::string_view msg = e.what();
            return wasmtime_trap_new(msg.data(), msg.size());
        }
    }

    /**
     * Invoke an async function.
     */
    static void invoke_async_host_fn(
      void*,
      wasmtime_caller_t* caller,
      const wasmtime_val_t* args,
      size_t nargs,
      wasmtime_val_t* results,
      size_t nresults,
      wasm_trap_t** trap_ret,
      wasmtime_async_continuation_t* continuation) {
        auto* context = wasmtime_caller_context(caller);
        auto* engine = static_cast<wasmtime_engine*>(
          wasmtime_context_get_data(context));
        auto* host_module = engine->get_module<Module>();
        // Keep this alive during the course of the host call incase it's used
        // as a parameter to a host function, they'll expect it's alive the
        // entire call. We'll move the ownership of this function into the host
        // future continuation.
        auto mem = std::make_unique<memory>(context);
        wasm_trap_t* trap = extract_memory(caller, mem.get());
        if (trap != nullptr) {
            *trap_ret = trap;
            // Return a callback that just says we're already done.
            continuation->callback = [](void*) { return true; };
            return;
        }

        ss::future<> host_future_result = do_invoke_async_host_fn(
          host_module, mem.get(), {args, nargs}, {results, nresults});

        // It's possible for the async function to be inlined in this fiber and
        // have completed synchronously, in that case we don't need to register
        // this future and can return that it's completed.
        if (host_future_result.available()) {
            if (host_future_result.failed()) {
                auto msg = ss::format(
                  "Failure executing host function: {}",
                  host_future_result.get_exception());
                *trap_ret = wasmtime_trap_new(msg.data(), msg.size());
            }
            continuation->callback = [](void*) { return true; };
            return;
        }

        using async_call_done = ss::bool_class<struct async_call_done_tag>;
        // NOLINTNEXTLINE(*owning-memory)
        auto* status = new async_call_done(false);

        engine->register_pending_host_function(
          std::move(host_future_result)
            .then_wrapped(
              [status, trap_ret, mem = std::move(mem)](ss::future<> fut) {
                  if (fut.failed()) {
                      auto msg = ss::format(
                        "Failure executing host function: {}",
                        fut.get_exception());
                      *trap_ret = wasmtime_trap_new(msg.data(), msg.size());
                  }
                  *status = async_call_done::yes;
              }));

        continuation->env = status;
        continuation->finalizer = [](void* env) {
            // NOLINTNEXTLINE(*owning-memory)
            delete static_cast<async_call_done*>(env);
        };
        continuation->callback = [](void* env) {
            async_call_done status = *static_cast<async_call_done*>(env);
            return status == async_call_done::yes;
        };
    }

    /**
     * Translate wasmtime types into our native types, then invoke the
     * corresponding host function, translating the response types into the
     * correct types.
     */
    static void do_invoke_sync_host_fn(
      Module* host_module,
      memory* mem,
      std::span<const wasmtime_val_t> args,
      std::span<wasmtime_val_t> results) {
        auto raw = to_raw_values(args);
        auto host_params = ffi::extract_parameters<ArgTypes...>(mem, raw, 0);
        if constexpr (std::is_void_v<ReturnType>) {
            std::apply(
              module_func,
              std::tuple_cat(
                std::make_tuple(host_module), std::move(host_params)));
        } else {
            ReturnType host_result = std::apply(
              module_func,
              std::tuple_cat(
                std::make_tuple(host_module), std::move(host_params)));
            results[0] = convert_to_wasmtime<ReturnType>(
              std::move(host_result));
        }
    }

    /**
     * Same as do_invoke_sync_host_fn but async.
     */
    static ss::future<> do_invoke_async_host_fn(
      Module* host_module,
      memory* mem,
      std::span<const wasmtime_val_t> args,
      std::span<wasmtime_val_t> results) {
        auto raw = to_raw_values(args);
        auto host_params = ffi::extract_parameters<ArgTypes...>(mem, raw, 0);
        using FutureType = typename ReturnType::value_type;
        if constexpr (std::is_void_v<FutureType>) {
            return ss::futurize_apply(
              module_func,
              std::tuple_cat(
                std::make_tuple(host_module), std::move(host_params)));
        } else {
            return ss::futurize_apply(
                     module_func,
                     std::tuple_cat(
                       std::make_tuple(host_module), std::move(host_params)))
              .then([results](FutureType host_future_result) {
                  results[0] = convert_to_wasmtime<FutureType>(
                    host_future_result);
              });
        }
    }

    /**
     * Our ABI contract expects a "memory" export from the module that we can
     * use to access the VM's memory space.
     */
    static wasm_trap_t* extract_memory(wasmtime_caller_t* caller, memory* mem) {
        constexpr std::string_view memory_export_name = "memory";
        wasmtime_extern_t extern_item;
        bool ok = wasmtime_caller_export_get(
          caller,
          memory_export_name.data(),
          memory_export_name.size(),
          &extern_item);
        if (!ok || extern_item.kind != WASMTIME_EXTERN_MEMORY) [[unlikely]] {
            constexpr std::string_view error = "Missing memory export";
            vlog(wasm_log.warn, "{}", error);
            return wasmtime_trap_new(error.data(), error.size());
        }
        mem->set_underlying_memory(&extern_item.of.memory);
        return nullptr;
    }
};

void register_wasi_module(wasmtime_linker_t* linker) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&wasi::preview1_module::name>::reg(linker, #name)
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

void register_transform_module(wasmtime_linker_t* linker) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&transform_module::name>::reg(linker, #name)
    REG_HOST_FN(read_batch_header);
    REG_HOST_FN(read_record);
    REG_HOST_FN(write_record);
#undef REG_HOST_FN
}
void register_sr_module(wasmtime_linker_t* linker) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&schema_registry_module::name>::reg(linker, #name)
    REG_HOST_FN(get_schema_definition);
    REG_HOST_FN(get_schema_definition_len);
    REG_HOST_FN(get_subject_schema);
    REG_HOST_FN(get_subject_schema_len);
    REG_HOST_FN(create_subject_schema);
#undef REG_HOST_FN
}

class wasmtime_engine_factory : public factory {
public:
    wasmtime_engine_factory(
      wasm_engine_t* engine,
      model::transform_metadata meta,
      ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>>
        preinitialized,
      schema_registry* sr,
      ss::logger* l)
      : _engine(engine)
      , _preinitialized(std::move(preinitialized))
      , _meta(std::move(meta))
      , _sr(sr)
      , _logger(l) {}

    // This can be invoked on any shard and must be thread safe.
    ss::future<ss::shared_ptr<engine>> make_engine() final {
        auto copy = co_await _preinitialized.copy();
        co_return ss::make_shared<wasmtime_engine>(
          _engine, _meta, std::move(copy), _sr, _logger);
    }

private:
    wasm_engine_t* _engine;
    ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>> _preinitialized;
    model::transform_metadata _meta;
    schema_registry* _sr;
    ss::logger* _logger;
};

class wasmtime_runtime : public runtime {
public:
    wasmtime_runtime(
      handle<wasm_engine_t, &wasm_engine_delete> h,
      std::unique_ptr<schema_registry> sr)
      : _engine(std::move(h))
      , _sr(std::move(sr)) {}

    ss::future<> start() override {
        co_await _alien_thread.start({.name = "wasm"});
        co_await ss::smp::invoke_on_all([] {
            // wasmtime needs some signals for it's handling, make sure we
            // unblock them.
            auto mask = ss::make_empty_sigset_mask();
            sigaddset(&mask, SIGSEGV);
            sigaddset(&mask, SIGILL);
            sigaddset(&mask, SIGFPE);
            ss::throw_pthread_error(
              ::pthread_sigmask(SIG_UNBLOCK, &mask, nullptr));
        });
    }

    ss::future<> stop() override { return _alien_thread.stop(); }

    ss::future<ss::shared_ptr<factory>> make_factory(
      model::transform_metadata meta, iobuf buf, ss::logger* logger) override {
        auto preinitialized = ss::make_lw_shared<preinitialized_instance>();
        co_await _alien_thread.submit([this, &meta, &buf, &preinitialized] {
            vlog(wasm_log.debug, "compiling wasm module {}", meta.name);
            // This can be a large contiguous allocation, however it happens
            // on an alien thread so it bypasses the seastar allocator.
            bytes b = iobuf_to_bytes(buf);
            wasmtime_module_t* user_module_ptr = nullptr;
            handle<wasmtime_error_t, wasmtime_error_delete> error{
              wasmtime_module_new(
                _engine.get(), b.data(), b.size(), &user_module_ptr)};
            check_error(error.get());
            handle<wasmtime_module_t, wasmtime_module_delete> user_module{
              user_module_ptr};
            wasm_log.info("Finished compiling wasm module {}", meta.name);

            handle<wasmtime_linker_t, wasmtime_linker_delete> linker{
              wasmtime_linker_new(_engine.get())};

            register_transform_module(linker.get());
            register_sr_module(linker.get());
            register_wasi_module(linker.get());

            wasmtime_instance_pre_t* preinitialized_ptr = nullptr;
            error.reset(wasmtime_linker_instantiate_pre(
              linker.get(), user_module.get(), &preinitialized_ptr));
            preinitialized->underlying.reset(preinitialized_ptr);
            check_error(error.get());
        });
        co_return ss::make_shared<wasmtime_engine_factory>(
          _engine.get(),
          std::move(meta),
          ss::make_foreign(std::move(preinitialized)),
          _sr.get(),
          logger);
    }

private:
    handle<wasm_engine_t, &wasm_engine_delete> _engine;
    std::unique_ptr<schema_registry> _sr;
    ssx::singleton_thread_worker _alien_thread;
};

} // namespace

std::unique_ptr<runtime> create_runtime(std::unique_ptr<schema_registry> sr) {
    wasm_config_t* config = wasm_config_new();

    // Spend more time compiling so that we can have faster code.
    wasmtime_config_cranelift_opt_level_set(config, WASMTIME_OPT_LEVEL_SPEED);
    // Fuel allows us to stop execution after some time.
    wasmtime_config_consume_fuel_set(config, true);
    // We want to enable memcopy and other efficent memcpy operators
    wasmtime_config_wasm_bulk_memory_set(config, true);
    // Our build disables this feature, so we don't need to turn it
    // off, otherwise we'd want to turn this off (it's on by default).
    // wasmtime_config_parallel_compilation_set(config, false);

    // Let wasmtime do the stack switching so we can run async host functions
    // and allow running out of fuel to pause the runtime.
    //
    // See the documentation for more information:
    // https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#asynchronous-wasm
    wasmtime_config_async_support_set(config, true);
    // Set max stack size to generally be as big as a contiguous memory region
    // we're willing to allocate in Redpanda.
    //
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    wasmtime_config_async_stack_size_set(config, 128_KiB);
    // The stack size needs to be less than the async stack size, and
    // whatever is difference between the two is how much host functions can
    // get, make sure to leave our own functions some room to execute.
    //
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    wasmtime_config_max_wasm_stack_set(config, 64_KiB);
    // This disables static memory, see:
    // https://docs.wasmtime.dev/contributing-architecture.html#linear-memory
    wasmtime_config_static_memory_maximum_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_guard_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_reserved_for_growth_set(config, 0_KiB);
    // Don't modify the unwind info as registering these symbols causes C++
    // exceptions to grab a lock in libgcc and deadlock the Redpanda process.
    wasmtime_config_native_unwind_info_set(config, false);

    handle<wasm_engine_t, &wasm_engine_delete> engine{
      wasm_engine_new_with_config(config)};
    return std::make_unique<wasmtime_runtime>(std::move(engine), std::move(sr));
}
} // namespace wasm::wasmtime
