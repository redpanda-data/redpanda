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
#include "wasmtime.h"

#include "allocator.h"
#include "base/type_traits.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "engine_probe.h"
#include "ffi.h"
#include "logger.h"
#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "model/transform.h"
#include "schema_registry_module.h"
#include "ssx/thread_worker.h"
#include "storage/parser_utils.h"
#include "transform_module.h"
#include "utils/human.h"
#include "utils/to_string.h"
#include "wasi.h"
#include "wasm/engine.h"
#include "wasm/errc.h"
#include "wasm/parser/parser.h"
#include "wasm/transform_probe.h"

#include <seastar/core/align.hh>
#include <seastar/core/future.hh>
#include <seastar/core/internal/cpu_profiler.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/optimized_optional.hh>

#include <absl/algorithm/container.h>
#include <absl/strings/escaping.h>
#include <fmt/ostream.h>

#include <alloca.h>
#include <csignal>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <pthread.h>
#include <unistd.h>
#include <utility>
#include <variant>
#include <wasm.h>
#include <wasmtime.h>

namespace wasm::wasmtime {

namespace {

// Our guidelines recommend allocating max 128KB at once. Since we also need to
// allocate a guard page, allocate 128KB-4KB of usable stack space.
constexpr size_t vm_stack_size = 124_KiB;
// The WebAssembly code gets at half the stack space for it's own work.
constexpr size_t max_vm_guest_stack_usage = 64_KiB;
// We allow for half the stack for host functions,
// plus a little wiggle room to not get too close
// to the guard page.
constexpr size_t max_host_function_stack_usage = vm_stack_size
                                                 - max_vm_guest_stack_usage
                                                 - 4_KiB;

// This amount of fuel allows the VM to run for about 1 millisecond for an
// infinite loop workload on x86_64.
constexpr uint64_t millisecond_fuel_amount = 2'000'000;

// The reserved memory for an instance of a WebAssembly VM.
//
// The wasmtime memory APIs don't allow us to pass information into an
// allocation request, so we use a thread local variable as a side channel to
// pass the allocated memory to wasmtime when we instantiate a module.
//
// We need this side channel because allocating memory is async, due to the case
// when memory is large enough we deallocate asynchronously. This variable is
// only set for a synchronous period where we pass the memory from where it was
// allocated into where wasmtime grabs host provided memory.
//
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static thread_local std::optional<heap_memory> prereserved_memory;

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) { fn(ptr); }
};
/** A unique_ptr that uses a free function as a deleter. */
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

/**
 * This is similar to std::out_ptr in C++23 when used with handles.
 *
 * Example usage:
 *
 * class foo;
 * int external_fn(foo** out_ptr);
 * void free_foo(foo*);
 *
 * int main() {
 *   handle<foo, free_foo> foo_ptr;
 *   int errno = external_fn(out_handle(foo_ptr));
 *   if (errno != 0) {
 *     throw ...;
 *   }
 *   // foo_ptr is set with the out ptr from external_fn
 * }
 */
template<typename T, auto fn>
class out_handle {
public:
    explicit out_handle(handle<T, fn>& out_handle)
      : _out_handle(&out_handle) {}
    out_handle(const out_handle&) = delete;
    out_handle(out_handle&&) = delete;
    out_handle& operator=(const out_handle&) = delete;
    out_handle& operator=(out_handle&&) = delete;
    ~out_handle() noexcept {
        _out_handle->reset(std::exchange(_raw_ptr, nullptr));
    }

    // NOLINTNEXTLINE
    operator T**() noexcept { return &_raw_ptr; }

private:
    handle<T, fn>* _out_handle;
    T* _raw_ptr = nullptr;
};

class wasmtime_runtime : public runtime {
public:
    explicit wasmtime_runtime(std::unique_ptr<schema::registry> sr);

    ss::future<> start(runtime::config c) override;

    ss::future<> stop() override;

    ss::future<ss::shared_ptr<factory>> make_factory(
      model::transform_metadata meta, model::wasm_binary_iobuf buf) override;

    ss::future<> validate(model::wasm_binary_iobuf buf) override;

    wasm_engine_t* engine() const;

    heap_allocator* heap_allocator();

    engine_probe_cache* engine_probe_cache();

    size_t per_invocation_fuel_amount() const;

private:
    void register_metrics();

    static wasmtime_error_t* allocate_stack_memory(
      void* env, size_t size, wasmtime_stack_memory_t* memory_ret);

    wasmtime_error_t*
    allocate_stack_memory(size_t size, wasmtime_stack_memory_t* memory_ret);

    // We don't have control over this API.
    // NOLINTBEGIN(bugprone-easily-swappable-parameters)
    static wasmtime_error_t* allocate_heap_memory(
      void* env,
      const wasm_memorytype_t* ty,
      size_t minimum,
      size_t maximum,
      size_t reserved_size_in_bytes,
      size_t guard_size_in_bytes,
      wasmtime_linear_memory_t* memory_ret);
    // NOLINTEND(bugprone-easily-swappable-parameters)

    wasmtime_error_t* allocate_heap_memory(
      heap_allocator::request, wasmtime_linear_memory_t* memory_ret);

    handle<wasm_engine_t, &wasm_engine_delete> _engine;
    std::unique_ptr<schema::registry> _sr;
    ssx::singleton_thread_worker _alien_thread;
    ss::sharded<wasm::heap_allocator> _heap_allocator;
    ss::sharded<stack_allocator> _stack_allocator;
    size_t _total_executable_memory = 0;
    metrics::public_metric_groups _public_metrics;
    ss::sharded<wasm::engine_probe_cache> _engine_probe_cache;
    size_t _per_invocation_fuel_amount = 0;
};

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

wasm_trap_t* make_trap(std::exception_ptr ex) {
    auto msg = ss::format("failure executing host function: {}", ex);
    return wasmtime_trap_new(msg.data(), msg.size());
};

void check_trap(const wasm_trap_t* trap) {
    if (!trap) {
        return;
    }
    wasm_name_t msg{.size = 0, .data = nullptr};
    wasm_trap_message(trap, &msg);
    std::stringstream sb;
    sb << std::string_view(msg.data, msg.size);
    wasm_byte_vec_delete(&msg);
    wasmtime_trap_code_t code = 0;
    if (wasmtime_trap_code(trap, &code)) {
        switch (static_cast<wasmtime_trap_code_enum>(code)) {
        case WASMTIME_TRAP_CODE_STACK_OVERFLOW:
            sb << " (code STACK_OVERFLOW)";
            break;
        case WASMTIME_TRAP_CODE_MEMORY_OUT_OF_BOUNDS:
            sb << " (code MEMORY_OUT_OF_BOUNDS)";
            break;
        case WASMTIME_TRAP_CODE_HEAP_MISALIGNED:
            sb << " (code HEAP_MISALIGNED)";
            break;
        case WASMTIME_TRAP_CODE_TABLE_OUT_OF_BOUNDS:
            sb << " (code TABLE_OUT_OF_BOUNDS)";
            break;
        case WASMTIME_TRAP_CODE_INDIRECT_CALL_TO_NULL:
            sb << " (code INDIRECT_CALL_TO_NULL)";
            break;
        case WASMTIME_TRAP_CODE_BAD_SIGNATURE:
            sb << " (code BAD_SIGNATURE)";
            break;
        case WASMTIME_TRAP_CODE_INTEGER_OVERFLOW:
            sb << " (code INTEGER_OVERFLOW)";
            break;
        case WASMTIME_TRAP_CODE_INTEGER_DIVISION_BY_ZERO:
            sb << " (code INTEGER_DIVISION_BY_ZERO)";
            break;
        case WASMTIME_TRAP_CODE_BAD_CONVERSION_TO_INTEGER:
            sb << " (code BAD_CONVERSION_TO_INTEGER)";
            break;
        case WASMTIME_TRAP_CODE_UNREACHABLE_CODE_REACHED:
            sb << " (code UNREACHABLE_CODE_REACHED)";
            break;
        case WASMTIME_TRAP_CODE_INTERRUPT:
            sb << " (code INTERRUPT)";
            break;
        case WASMTIME_TRAP_CODE_OUT_OF_FUEL:
            sb << " (code OUT_OF_FUEL)";
            break;
        default:
            break;
        }
    }
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
          base::unsupported_type<T>::value, "Unsupported wasm result type");
    }
}

class memory : public ffi::memory {
public:
    explicit memory(wasmtime_context_t* ctx)
      : _ctx(ctx)
      , _underlying() {}

    void* translate_raw(ffi::ptr guest_ptr, uint32_t len) final {
        size_t memory_size = wasmtime_memory_data_size(_ctx, &_underlying);
        // Prevent overflow by upgrading to a larger type.
        size_t end_read_addr = guest_ptr();
        end_read_addr += len;
        if (end_read_addr > memory_size) [[unlikely]] {
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
 * The declared limits of a WebAssembly's Linear Memory.
 *
 * The bounds here are inclusive.
 */
struct memory_limits {
    uint32_t minimum = 0;
    uint32_t maximum = std::numeric_limits<uint32_t>::max();
};

memory_limits lookup_memory_limits(const wasmtime_module_t* mod) {
    wasm_exporttype_vec_t exports;
    wasm_exporttype_vec_new_empty(&exports);
    auto _ = ss::defer([&exports] { wasm_exporttype_vec_delete(&exports); });
    wasmtime_module_exports(mod, &exports);
    for (const wasm_exporttype_t* module_export :
         std::span(exports.data, exports.size)) {
        const wasm_externtype_t* extern_type = wasm_exporttype_type(
          module_export);
        if (wasm_externtype_kind(extern_type) != WASM_EXTERN_MEMORY) {
            continue;
        }
        const wasm_name_t* raw_name = wasm_exporttype_name(module_export);
        auto name = std::string_view(raw_name->data, raw_name->size);
        if (name != "memory") {
            throw wasm_exception(
              ss::format(
                "invalid memory export name: \"{}\"", absl::CHexEscape(name)),
              errc::user_code_failure);
        }
        const wasm_memorytype_t* memory_type
          = wasm_externtype_as_memorytype_const(extern_type);
        if (wasmtime_memorytype_is64(memory_type)) {
            throw wasm_exception(
              ss::format("invalid 64bit memory"), errc::user_code_failure);
        }
        uint64_t minimum = wasmtime_memorytype_minimum(memory_type);
        uint64_t maximum = std::numeric_limits<uint32_t>::max();
        wasmtime_memorytype_maximum(memory_type, &maximum);
        return {
          .minimum = static_cast<uint32_t>(minimum),
          .maximum = static_cast<uint32_t>(maximum),
        };
    }
    throw wasm_exception(
      "wasm module missing memory export", errc::user_code_failure);
}

/**
 * Preinitialized instances only need a store to be plugged in and start
 * running. All compilation including any trampolines for host functions have
 * already been compiled in and no other code generation/compilation needs to
 * happen.
 *
 * Creating instances from this is as fast as allocating the memory needed and
 * running any startup functions for the module.
 */
class preinitialized_instance {
public:
    preinitialized_instance() = default;
    preinitialized_instance(const preinitialized_instance&) = delete;
    preinitialized_instance(preinitialized_instance&&) = delete;
    preinitialized_instance& operator=(const preinitialized_instance&) = delete;
    preinitialized_instance& operator=(preinitialized_instance&&) = delete;
    ~preinitialized_instance() {
        if (_cleanup_fn) {
            _cleanup_fn();
        }
    }

    wasmtime_instance_pre_t* get() noexcept { return _underlying.get(); }

    memory_limits mem_limits() const { return _memory_limits; }

private:
    friend class wasmtime_runtime;
    handle<wasmtime_instance_pre_t, wasmtime_instance_pre_delete> _underlying
      = nullptr;
    memory_limits _memory_limits;
    ss::noncopyable_function<void() noexcept> _cleanup_fn;
};

absl::flat_hash_map<ss::sstring, ss::sstring>
make_environment_vars(const model::transform_metadata& meta) {
    absl::flat_hash_map<ss::sstring, ss::sstring> env = meta.environment;
    env.emplace("REDPANDA_INPUT_TOPIC", meta.input_topic.tp());
    for (size_t i = 0; i < meta.output_topics.size(); ++i) {
        env.emplace(
          ss::format("REDPANDA_OUTPUT_TOPIC_{}", i),
          meta.output_topics[i].tp());
    }
    return env;
}

class wasmtime_engine : public engine {
public:
    wasmtime_engine(
      wasmtime_runtime* runtime,
      model::transform_metadata metadata,
      ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>>
        preinitialized,
      schema::registry* sr,
      std::unique_ptr<wasm::logger> logger)
      : _runtime(runtime)
      , _meta(std::move(metadata))
      , _preinitialized(std::move(preinitialized))
      , _probe(runtime->engine_probe_cache()->make_probe(_meta.name()))
      , _logger(std::move(logger))
      , _sr_module(sr)
      , _wasi_module(
          {_meta.name()}, make_environment_vars(_meta), _logger.get())
      , _transform_module(&_wasi_module) {}
    wasmtime_engine(const wasmtime_engine&) = delete;
    wasmtime_engine& operator=(const wasmtime_engine&) = delete;
    wasmtime_engine(wasmtime_engine&&) = delete;
    wasmtime_engine& operator=(wasmtime_engine&&) = delete;
    ~wasmtime_engine() override = default;

    ss::future<> start() final {
        _transform_module.start();
        co_await create_instance();
        _main_task = initialize_wasi();
        co_await _transform_module.await_ready();
        report_memory_usage();
    }

    ss::future<> stop() final {
        ss::future<> main = std::exchange(_main_task, ss::now());
        _transform_module.stop(std::make_exception_ptr(
          wasm_exception("vm was shutdown", errc::engine_shutdown)));
        co_await std::move(main);
        // Deleting the store invalidates the instance and actually frees the
        // memory for the underlying instance.
        _store = nullptr;
        vassert(
          !_pending_host_function,
          "pending host functions should be awaited upon before stopping the "
          "engine");
        _probe.report_max_memory(0);
        _probe.report_memory_usage(0);
    }

    ss::future<> transform(
      model::record_batch batch,
      transform_probe* probe,
      transform_callback cb) override {
        vlog(wasm_log.trace, "Transforming batch: {}", batch.header());
        if (batch.record_count() == 0) {
            co_return;
        }
        if (batch.compressed()) {
            batch = co_await storage::internal::decompress_batch(
              std::move(batch));
        }
        ss::future<> fut = co_await ss::coroutine::as_future(
          invoke_transform(std::move(batch), probe, std::move(cb)));
        report_memory_usage();
        if (fut.failed()) {
            probe->transform_error();
            std::rethrow_exception(fut.get_exception());
        }
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
              base::unsupported_type<T>::value, "unsupported module");
        }
    }

    // Register that a pending async host function is happening, this future
    // must never fail.
    void register_pending_host_function(ss::future<> fut) noexcept {
        _pending_host_function.emplace(std::move(fut));
    }

private:
    uint64_t memory_usage_size_bytes() const {
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

    void report_memory_usage() {
        _probe.report_memory_usage(memory_usage_size_bytes());
    }

    void reset_fuel(wasmtime_context_t* ctx) {
        handle<wasmtime_error_t, wasmtime_error_delete> error(
          wasmtime_context_set_fuel(
            ctx, _runtime->per_invocation_fuel_amount()));
        check_error(error.get());
    }

    /**
     * Run a WebAssembly call until completion, after the "future" is done
     * completing the results from the computation (as well as errors) are
     * available.
     */
    ss::future<>
    execute(handle<wasmtime_call_future_t, wasmtime_call_future_delete> fut) {
        // Poll the call future to completion, yielding to the scheduler when
        // the future yields.
        auto start = ss::steady_clock_type::now();
        // Disable profiling backtraces inside the VM - at the time of writing
        // backtraces lead to segfaults causing deadlock in Seastar's signal
        // handlers.
        auto _ = ss::internal::scoped_disable_profile_temporarily();
        while (!wasmtime_call_future_poll(fut.get())) {
            // Re-enable stacktraces before we yield control to the scheduler.
            ss::internal::profiler_drop_stacktraces(false);
            auto end = ss::steady_clock_type::now();
            _probe.increment_cpu_time(end - start);
            if (_pending_host_function) {
                auto host_future = std::exchange(_pending_host_function, {});
                co_await std::move(host_future).value();
            } else {
                co_await ss::coroutine::maybe_yield();
            }
            start = ss::steady_clock_type::now();
            // Disable stacktraces as we enter back into Wasmtime
            ss::internal::profiler_drop_stacktraces(true);
        }
        auto end = ss::steady_clock_type::now();
        _probe.increment_cpu_time(end - start);
    }

    ss::future<> create_instance() {
        // The underlying "data" for this store is our engine, which is what
        // allows our host functions to access the actual module for that host
        // function.
        handle<wasmtime_store_t, wasmtime_store_delete> store{
          wasmtime_store_new(
            _runtime->engine(), /*data=*/this, /*finalizer=*/nullptr)};
        // Tables are backed by a vector holding pointers, so ensure the maximum
        // allocation is under our recommended limit.
        constexpr size_t table_element_size = sizeof(void*);
        constexpr size_t max_table_elements = 128_KiB / table_element_size;
        // We only ever create a single instance within this store, and we
        // expect that modules only have a single table and a single memory
        // instance declared.
        uint32_t max_memory_size = _runtime->heap_allocator()->max_size();
        wasmtime_store_limiter(
          store.get(),
          /*memory_size=*/max_memory_size,
          /*table_elements=*/max_table_elements,
          /*instances=*/1,
          /*tables=*/1,
          /*memories=*/1);
        _probe.report_max_memory(max_memory_size);
        _probe.report_memory_usage(0);
        auto* context = wasmtime_store_context(store.get());

        wasmtime_context_fuel_async_yield_interval(
          context, millisecond_fuel_amount);

        reset_fuel(context);

        _wasi_module.set_walltime(model::timestamp::min());

        auto requested = _preinitialized->mem_limits();

        // Wait for memory to be available if the allocator is currently freeing
        // memory.
        auto memory = co_await _runtime->heap_allocator()->allocate({
          .minimum = requested.minimum,
          .maximum = std::min(requested.maximum, max_memory_size),
        });

        if (!memory) {
            throw wasm_exception(
              ss::format(
                "unable to allocate memory within requested bounds: [{}, {}]",
                requested.minimum,
                requested.maximum),
              errc::engine_creation_failure);
        }

        // Assign our allocated memory to thread local storage so we can bypass
        // it into the allocator for the VM
        prereserved_memory = std::move(memory);

        // The wasm spec has a feature that a module can specify a startup
        // function that is run on start.
        handle<wasmtime_error_t, wasmtime_error_delete> error;
        handle<wasm_trap_t, wasm_trap_delete> trap;

        {
            // These pointers don't get set until `fut` is completed.
            auto trap_out = out_handle(trap);
            auto error_out = out_handle(error);

            handle<wasmtime_call_future_t, wasmtime_call_future_delete> fut{
              wasmtime_instance_pre_instantiate_async(
                _preinitialized->get(),
                context,
                &_instance,
                trap_out,
                error_out)};

            // The first poll of the call future is what allocates memory/stack
            // so we need to do that before we assert that it was used.
            ss::future<> ready = execute(std::move(fut));

            if (prereserved_memory) {
                vlog(
                  wasm_log.error,
                  "did not use prereserved memory when instantiating wasm vm");
                _runtime->heap_allocator()->deallocate(
                  std::exchange(prereserved_memory, std::nullopt).value(), 0);
            }

            co_await std::move(ready);
        }

        // Now that the call future has returned as completed, we can assume the
        // out pointers have been set, we need to check them for errors.
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
        handle<wasmtime_error_t, wasmtime_error_delete> error;
        handle<wasm_trap_t, wasm_trap_delete> trap;

        {
            // These out pointers are not set until `fut` completes.
            auto error_out = out_handle(error);
            auto trap_out = out_handle(trap);

            handle<wasmtime_call_future_t, wasmtime_call_future_delete> fut{
              wasmtime_func_call_async(
                ctx,
                func,
                args.data(),
                args.size(),
                results.data(),
                results.size(),
                trap_out,
                error_out)};

            co_await execute(std::move(fut));
        }

        // Now that the call future has returned as completed, we can assume the
        // out pointers have been set, we need to check them for errors.
        // Any results have been written to results and it's on the caller to
        // check.
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
              "Missing wasi _start function", errc::user_code_failure);
        }
        vlog(wasm_log.info, "starting wasm vm {}", _meta.name());
        std::exception_ptr ex;
        try {
            co_await call_host_func(ctx, &start.of.func, {}, {});
        } catch (...) {
            ex = std::current_exception();
            // In the stop method, _main_task is exchanged for a ready future,
            // this means that're shutting down and we expect the VM to be
            // shutdown, so prevent the log spam in this case.
            //
            // In reality it'd be better if we could plumb some custom data
            // through the wasm runtime host errors for this. But we can't do
            // that at the moment.
            if (!_main_task.available()) {
                vlog(wasm_log.warn, "wasm vm failed: {}", ex);
            }
        }
        vlog(wasm_log.info, "wasm vm {} finished", _meta.name());
        if (!ex) {
            ex = std::make_exception_ptr(
              wasm_exception("vm has exited", errc::engine_not_running));
        }
        _transform_module.stop(ex);
    }

    ss::future<> invoke_transform(
      model::record_batch batch, transform_probe* p, transform_callback cb) {
        class callback_impl final : public record_callback {
        public:
            callback_impl(
              wasmtime_context_t* context,
              size_t fuel_amt,
              transform_callback cb,
              transform_probe* p)
              : _context(context)
              , _fuel_amt(fuel_amt)
              , _cb(std::move(cb))
              , _probe(p) {}

            void pre_record() final {
                handle<wasmtime_error_t, wasmtime_error_delete> error(
                  wasmtime_context_set_fuel(_context, _fuel_amt));
                check_error(error.get());
                _measurement = _probe->latency_measurement();
            }

            ss::future<write_success> emit(
              std::optional<model::topic_view> topic,
              model::transformed_data data) final {
                return _cb(topic, std::move(data));
            }

            void post_record() final { _measurement = nullptr; }

        private:
            wasmtime_context_t* _context;
            size_t _fuel_amt;
            transform_callback _cb;
            transform_probe* _probe;
            std::unique_ptr<transform_probe::hist_t::measurement> _measurement;
        };
        callback_impl callback(
          wasmtime_store_context(_store.get()),
          _runtime->per_invocation_fuel_amount(),
          std::move(cb),
          p);

        co_await _transform_module.for_each_record_async(
          std::move(batch), &callback);
    }

    wasmtime_runtime* _runtime;
    model::transform_metadata _meta;
    ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>> _preinitialized;
    engine_probe _probe;
    std::unique_ptr<wasm::logger> _logger;

    schema_registry_module _sr_module;
    wasi::preview1_module _wasi_module;
    transform_module _transform_module;

    // The following state is only valid if there is a non-null store.
    handle<wasmtime_store_t, wasmtime_store_delete> _store;
    wasmtime_instance_t _instance{};
    std::optional<ss::future<>> _pending_host_function;
    ss::future<> _main_task = ss::now();
};

// If strict stack checking is configured
//
// Strict stack checking ensures that our host functions don't use too much
// stack, even if in our tests they leave plenty of extra stack space (not
// all VM guest programs will be so nice).
struct strict_stack_config {
    ss::sharded<stack_allocator>* allocator;

    bool enabled() const { return allocator != nullptr; }
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
    static void reg(
      wasmtime_linker_t* linker,
      std::string_view function_name,
      const strict_stack_config& ssc) {
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
                ssc.enabled() ? &invoke_async_host_fn_with_strict_stack_checking
                              : &invoke_async_host_fn,
                /*data=*/ssc.allocator,
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
                ssc.enabled() ? &invoke_sync_host_fn_with_strict_stack_checking
                              : &invoke_sync_host_fn,
                /*data=*/ssc.allocator,
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
        } catch (...) {
            return make_trap(std::current_exception());
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
                *trap_ret = make_trap(host_future_result.get_exception());
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
                      *trap_ret = make_trap(fut.get_exception());
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
        try {
            auto raw = to_raw_values(args);
            auto host_params = ffi::extract_parameters<ArgTypes...>(
              mem, raw, 0);
            using FutureType = typename ReturnType::value_type;
            if constexpr (std::is_void_v<FutureType>) {
                return std::apply(
                  module_func,
                  std::tuple_cat(
                    std::make_tuple(host_module), std::move(host_params)));
            } else {
                return std::apply(
                         module_func,
                         std::tuple_cat(
                           std::make_tuple(host_module),
                           std::move(host_params)))
                  .then([results](FutureType host_future_result) {
                      // This is safe to write too because wasmtime ensures the
                      // result is kept alive until the future completes.
                      results[0] = convert_to_wasmtime<FutureType>(
                        host_future_result);
                  });
            }
        } catch (...) {
            return ss::current_exception_as_future();
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

    static wasm_trap_t* invoke_sync_host_fn_with_strict_stack_checking(
      void* env,
      wasmtime_caller_t* caller,
      const wasmtime_val_t* args,
      size_t nargs,
      wasmtime_val_t* results,
      size_t nresults) {
        auto* allocator = static_cast<ss::sharded<stack_allocator>*>(env);
        uint8_t dummy_stack_var = 0;
        auto bounds = allocator->local().stack_bounds_for_address(
          &dummy_stack_var);
        if (!bounds) {
            vlog(wasm_log.warn, "can't find vm stack!");
            return invoke_sync_host_fn(
              nullptr, caller, args, nargs, results, nresults);
        }
        // Ensure that we only use `max_host_function_stack_usage` by
        // allocing enough to call the host function with only that much
        // stack space left.
        std::ptrdiff_t stack_left = (&dummy_stack_var) - bounds->bottom;
        void* stack_ptr = ::alloca(stack_left - max_host_function_stack_usage);
        // Prevent the alloca from being optimized away by logging the result.
        vlog(
          wasm_log.trace,
          "alloca-ing {}, stack left: {}, alloca address: {}, stack bounds: {}",
          human::bytes(stack_left - max_host_function_stack_usage),
          human::bytes(stack_left),
          stack_ptr,
          bounds);
        return invoke_sync_host_fn(
          nullptr, caller, args, nargs, results, nresults);
    }

    static void invoke_async_host_fn_with_strict_stack_checking(
      void* env,
      wasmtime_caller_t* caller,
      const wasmtime_val_t* args,
      size_t nargs,
      wasmtime_val_t* results,
      size_t nresults,
      wasm_trap_t** trap_ret,
      wasmtime_async_continuation_t* continuation) {
        auto* allocator = static_cast<ss::sharded<stack_allocator>*>(env);
        uint8_t dummy_stack_var = 0;
        auto bounds = allocator->local().stack_bounds_for_address(
          &dummy_stack_var);
        if (!bounds) {
            vlog(wasm_log.warn, "can't find vm stack!");
            invoke_async_host_fn(
              nullptr,
              caller,
              args,
              nargs,
              results,
              nresults,
              trap_ret,
              continuation);
            return;
        }
        // Ensure that we only use `max_host_function_stack_usage` by
        // allocing enough to call the host function with only that much
        // stack space left.
        std::ptrdiff_t stack_left = (&dummy_stack_var) - bounds->bottom;
        void* stack_ptr = ::alloca(stack_left - max_host_function_stack_usage);
        // Prevent the alloca from being optimized away by logging the result.
        vlog(
          wasm_log.trace,
          "alloca-ing {}, stack left: {}, alloca address: {}, stack bounds: {}",
          human::bytes(stack_left - max_host_function_stack_usage),
          human::bytes(stack_left),
          stack_ptr,
          bounds);
        invoke_async_host_fn(
          nullptr,
          caller,
          args,
          nargs,
          results,
          nresults,
          trap_ret,
          continuation);
    }
};

void register_wasi_module(
  wasmtime_linker_t* linker, const strict_stack_config& ssc) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&wasi::preview1_module::name>::reg(linker, #name, ssc)
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
  wasmtime_linker_t* linker, const strict_stack_config& ssc) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&transform_module::name>::reg(linker, #name, ssc)
    REG_HOST_FN(check_abi_version_1);
    REG_HOST_FN(check_abi_version_2);
    REG_HOST_FN(read_batch_header);
    REG_HOST_FN(read_next_record);
    REG_HOST_FN(write_record);
    REG_HOST_FN(write_record_with_options);
#undef REG_HOST_FN
}

void register_sr_module(
  wasmtime_linker_t* linker, const strict_stack_config& ssc) {
    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define REG_HOST_FN(name)                                                      \
    host_function<&schema_registry_module::name>::reg(linker, #name, ssc)
    REG_HOST_FN(check_abi_version_0);
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
      wasmtime_runtime* runtime,
      model::transform_metadata meta,
      ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>>
        preinitialized,
      schema::registry* sr)
      : _runtime(runtime)
      , _preinitialized(std::move(preinitialized))
      , _meta(std::move(meta))
      , _sr(sr) {}

    // This can be invoked on any shard and must be thread safe.
    ss::future<ss::shared_ptr<engine>>
    make_engine(std::unique_ptr<wasm::logger> logger) final {
        auto copy = co_await _preinitialized.copy();
        co_return ss::make_shared<wasmtime_engine>(
          _runtime, _meta, std::move(copy), _sr, std::move(logger));
    }

private:
    wasmtime_runtime* _runtime;
    ss::foreign_ptr<ss::lw_shared_ptr<preinitialized_instance>> _preinitialized;
    model::transform_metadata _meta;
    schema::registry* _sr;
};

wasmtime_runtime::wasmtime_runtime(std::unique_ptr<schema::registry> sr)
  : _sr(std::move(sr)) {
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

    // Let wasmtime do the stack switching so we can run async host
    // functions and allow running out of fuel to pause the runtime.
    //
    // See the documentation for more information:
    // https://docs.wasmtime.dev/api/wasmtime/struct.Config.html#asynchronous-wasm
    wasmtime_config_async_support_set(config, true);
    // Set max stack size to generally be as big as a contiguous memory
    // region we're willing to allocate in Redpanda.
    wasmtime_config_async_stack_size_set(config, vm_stack_size);
    // The stack size needs to be less than the async stack size, and
    // whatever is difference between the two is how much host functions can
    // get, make sure to leave our own functions some room to execute.
    wasmtime_config_max_wasm_stack_set(config, max_vm_guest_stack_usage);
    // This disables static memory, see:
    // https://docs.wasmtime.dev/contributing-architecture.html#linear-memory
    wasmtime_config_static_memory_maximum_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_guard_size_set(config, 0_KiB);
    wasmtime_config_dynamic_memory_reserved_for_growth_set(config, 0_KiB);
    // Don't modify the unwind info as registering these symbols causes C++
    // exceptions to grab a lock in libgcc and deadlock the Redpanda
    // process.
    wasmtime_config_native_unwind_info_set(config, false);
    // Copy on write memory is only used when memory is `mmap`ed and cannot be
    // used with custom allocators, so let's just disable the generation of the
    // COW images since we don't use them.
    wasmtime_config_memory_init_cow_set(config, false);

    wasmtime_memory_creator_t memory_creator = {
      .env = this,
      .new_memory = &wasmtime_runtime::allocate_heap_memory,
      .finalizer = nullptr,
    };
    wasmtime_config_host_memory_creator_set(config, &memory_creator);

    wasmtime_stack_creator_t stack_creator = {
      .env = this,
      .new_stack = &wasmtime_runtime::allocate_stack_memory,
      .finalizer = nullptr,
    };
    wasmtime_config_host_stack_creator_set(config, &stack_creator);

    _engine.reset(wasm_engine_new_with_config(config));
}

ss::future<> wasmtime_runtime::start(runtime::config c) {
    using namespace std::chrono_literals;
    _per_invocation_fuel_amount = (c.cpu.per_invocation_timeout / 1ms)
                                  * millisecond_fuel_amount;

    size_t page_size = ::getpagesize();
    size_t aligned_pool_size = ss::align_down(
      c.heap_memory.per_core_pool_size_bytes, page_size);
    if (aligned_pool_size == 0) {
        throw std::runtime_error(ss::format(
          "aligned per core wasm memory pool size must be >0 "
          "(page_size={}, pool_size={})",
          page_size,
          c.heap_memory.per_core_pool_size_bytes));
    }
    size_t aligned_instance_limit = ss::align_down(
      c.heap_memory.per_engine_memory_limit, page_size);
    if (aligned_instance_limit == 0) {
        throw std::runtime_error(ss::format(
          "aligned per wasm engine memory limit must be >0 (page_size={}, "
          "limit={})",
          page_size,
          c.heap_memory.per_engine_memory_limit));
    }
    size_t num_heaps = aligned_pool_size / aligned_instance_limit;
    if (num_heaps == 0) {
        throw std::runtime_error("must allow at least one wasm heap");
    }

    // The maximum amount of memory we memset in a single task.
    //
    // In an effort to prevent reactor stalls, we memset only this size of
    // chunk, otherwise we yield control.
    constexpr static size_t memset_chunk_size = 10_MiB;

    co_await _heap_allocator.start(heap_allocator::config{
      .heap_memory_size = aligned_instance_limit,
      .num_heaps = num_heaps,
      .memset_chunk_size = memset_chunk_size,
    });
    co_await _stack_allocator.start(stack_allocator::config{
      .tracking_enabled = c.stack_memory.debug_host_stack_usage,
    });
    co_await _alien_thread.start({.name = "wasm"});
    co_await ss::smp::invoke_on_all([] {
        // wasmtime needs some signals for it's handling, make sure we
        // unblock them.
        auto mask = ss::make_empty_sigset_mask();
        sigaddset(&mask, SIGSEGV);
        sigaddset(&mask, SIGILL);
        sigaddset(&mask, SIGFPE);
        ss::throw_pthread_error(::pthread_sigmask(SIG_UNBLOCK, &mask, nullptr));
    });
    co_await _engine_probe_cache.start();
    register_metrics();
}

void wasmtime_runtime::register_metrics() {
    namespace sm = ss::metrics;
    _public_metrics.add_group(
      prometheus_sanitize::metrics_name("wasm_binary"),
      {
        sm::make_gauge(
          "executable_memory_usage",
          [this] { return _total_executable_memory; },
          sm::description("The amount of executable memory used for "
                          "WebAssembly binaries"))
          .aggregate({ss::metrics::shard_label}),
      });
}

ss::future<> wasmtime_runtime::stop() {
    _public_metrics.clear();
    co_await _engine_probe_cache.stop();
    co_await _alien_thread.stop();
    co_await _heap_allocator.stop();
    co_await _stack_allocator.stop();
}

ss::future<ss::shared_ptr<factory>> wasmtime_runtime::make_factory(
  model::transform_metadata meta, model::wasm_binary_iobuf buf) {
    auto preinitialized = ss::make_lw_shared<preinitialized_instance>();

    // Enable strict stack checking only if tracking is enabled.
    //
    // (strict stack checking ensures our host functions don't use too much
    // stack space, even when guests leave extra stack).
    strict_stack_config ssc = {
      .allocator = _stack_allocator.local().tracking_enabled()
                     ? &_stack_allocator
                     : nullptr,
    };
    size_t memory_usage_size = co_await _alien_thread.submit(
      [this, &meta, buf = buf().get(), &preinitialized, &ssc] {
          vlog(wasm_log.debug, "compiling wasm module {}", meta.name);
          // This can be a large contiguous allocation, however it happens
          // on an alien thread so it bypasses the seastar allocator.
          bytes b = iobuf_to_bytes(*buf);
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

          register_transform_module(linker.get(), ssc);
          register_sr_module(linker.get(), ssc);
          register_wasi_module(linker.get(), ssc);

          error.reset(wasmtime_linker_instantiate_pre(
            linker.get(),
            user_module.get(),
            out_handle(preinitialized->_underlying)));
          preinitialized->_memory_limits = lookup_memory_limits(
            user_module.get());
          check_error(error.get());

          size_t start = 0, end = 0;
          // NOLINTBEGIN(*-reinterpret-*)
          wasmtime_module_image_range(
            user_module.get(),
            reinterpret_cast<void**>(&start),
            reinterpret_cast<void**>(&end));
          // NOLINTEND(*-reinterpret-*)
          return end - start;
      });
    _total_executable_memory += memory_usage_size;
    preinitialized->_cleanup_fn = [this, memory_usage_size]() noexcept {
        _total_executable_memory -= memory_usage_size;
    };
    co_return ss::make_shared<wasmtime_engine_factory>(
      this,
      std::move(meta),
      ss::make_foreign(std::move(preinitialized)),
      _sr.get());
}

wasm_engine_t* wasmtime_runtime::engine() const { return _engine.get(); }
heap_allocator* wasmtime_runtime::heap_allocator() {
    return &_heap_allocator.local();
}
engine_probe_cache* wasmtime_runtime::engine_probe_cache() {
    return &_engine_probe_cache.local();
}
size_t wasmtime_runtime::per_invocation_fuel_amount() const {
    return _per_invocation_fuel_amount;
}

wasmtime_error_t* wasmtime_runtime::allocate_stack_memory(
  void* env, size_t size, wasmtime_stack_memory_t* memory_ret) {
    auto* runtime = static_cast<wasmtime_runtime*>(env);
    return runtime->allocate_stack_memory(size, memory_ret);
}

wasmtime_error_t* wasmtime_runtime::allocate_stack_memory(
  size_t size, wasmtime_stack_memory_t* memory_ret) {
    auto stack = _stack_allocator.local().allocate(size);
    struct vm_stack {
        stack_memory underlying;
        wasm::stack_allocator* allocator;
    };
    memory_ret->env = new vm_stack{
      .underlying = std::move(stack),
      .allocator = &_stack_allocator.local(),
    };
    memory_ret->get_stack_memory = [](void* env, size_t* len_ret) -> uint8_t* {
        auto* mem = static_cast<vm_stack*>(env);
        *len_ret = mem->underlying.size();
        return mem->underlying.bounds().top;
    };
    memory_ret->finalizer = [](void* env) {
        auto* mem = static_cast<vm_stack*>(env);
        mem->allocator->deallocate(std::move(mem->underlying));
        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
        delete mem;
    };
    return nullptr;
}

// NOLINTBEGIN(bugprone-easily-swappable-parameters)
wasmtime_error_t* wasmtime_runtime::allocate_heap_memory(
  void* env,
  const wasm_memorytype_t* ty,
  size_t minimum,
  size_t maximum,
  size_t reserved_size_in_bytes,
  size_t guard_size_in_bytes,
  wasmtime_linear_memory_t* memory_ret) {
    // NOLINTEND(bugprone-easily-swappable-parameters)
    vassert(
      !wasmtime_memorytype_is64(ty),
      "we only support 32bit addressable memory");
    vassert(
      reserved_size_in_bytes == 0,
      "this value should be set to 0 according to the config");
    vassert(
      guard_size_in_bytes == 0,
      "this value should be set to 0 according to the config");
    auto* runtime = static_cast<wasmtime_runtime*>(env);
    return runtime->allocate_heap_memory({minimum, maximum}, memory_ret);
}

wasmtime_error_t* wasmtime_runtime::allocate_heap_memory(
  heap_allocator::request req, wasmtime_linear_memory_t* memory_ret) {
    auto memory = std::exchange(prereserved_memory, std::nullopt);
    if (!memory) {
        vlog(
          wasm_log.error,
          "attempted to allocate heap memory without any memory in thread "
          "local storage");
        return wasmtime_error_new("preserved memory was missing");
    }
    if (memory->size < req.minimum || memory->size > req.maximum) {
        auto msg = ss::format(
          "allocated memory (size={}) was not within requested bounds: [{}, "
          "{}]",
          memory->size,
          req.minimum,
          req.maximum);
        // return the memory we used back to the allocator
        _heap_allocator.local().deallocate(std::move(*memory), 0);
        return wasmtime_error_new(msg.c_str());
    }
    struct linear_memory {
        heap_memory underlying;
        size_t used_memory;
        wasm::heap_allocator* allocator;
    };
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    memory_ret->env = new linear_memory{
      .underlying = *std::move(memory),
      .used_memory = req.minimum,
      .allocator = &_heap_allocator.local(),
    };
    memory_ret->finalizer = [](void* env) {
        auto* mem = static_cast<linear_memory*>(env);
        mem->allocator->deallocate(
          std::move(mem->underlying), /*used_amount=*/mem->used_memory);
        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
        delete mem;
    };
    memory_ret->get_memory =
      // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
      [](void* env, size_t* byte_size, size_t* max_byte_size) {
          auto* mem = static_cast<linear_memory*>(env);
          *byte_size = mem->used_memory;
          *max_byte_size = mem->underlying.size;
          return mem->underlying.data.get();
      };
    memory_ret->grow_memory =
      [](void* env, size_t new_size) -> wasmtime_error_t* {
        auto* mem = static_cast<linear_memory*>(env);
        if (new_size <= mem->underlying.size) {
            mem->used_memory = std::max(new_size, mem->used_memory);
            return nullptr;
        }
        auto msg = ss::format(
          "unable to grow memory past {} to {}",
          human::bytes(double(mem->underlying.size)),
          human::bytes(double(new_size)));
        return wasmtime_error_new(msg.c_str());
    };
    return nullptr;
}

bool is_wasi_main(const parser::module_export& mod_export) {
    // Expect a function called _start with no parameters or results.
    return mod_export
           == parser::module_export{
             .item_name = "_start",
             .description = parser::declaration::function{}};
}

bool is_exported_memory(const parser::module_export& mod_export) {
    return mod_export.item_name == "memory"
           && std::holds_alternative<parser::declaration::memory>(
             mod_export.description);
}

bool is_transform_abi_check_fn(const parser::module_import& mod_import) {
    constexpr std::array version = {1, 2};
    return absl::c_any_of(version, [&mod_import](int version) {
        return mod_import
               == parser::module_import{
                 .module_name = ss::sstring(transform_module::name),
                 .item_name = ss::format("check_abi_version_{}", version),
                 .description = parser::declaration::function{}};
    });
}

// Schema registry is optional, so only check that the function is not using an
// unsupported version.
bool is_invalid_sr_abi_check_fn(const parser::module_import& mod_import) {
    if (mod_import.module_name != schema_registry_module::name) {
        return false;
    }
    if (!mod_import.item_name.starts_with("check_abi_version_")) {
        return false;
    }
    const auto void_fn = parser::import_description{
      parser::declaration::function{}};
    if (mod_import.description != void_fn) {
        return true;
    }
    return mod_import.item_name != "check_abi_version_0";
}

ss::future<> wasmtime_runtime::validate(model::wasm_binary_iobuf buf) {
    parser::module_declarations decls;
    try {
        decls = co_await parser::extract_declarations(std::move(*buf()));
    } catch (const parser::module_too_large_exception& ex) {
        vlog(wasm_log.warn, "invalid module (too large): {}", ex);
        throw wasm_exception(ex.what(), errc::invalid_module);
    } catch (const parser::parse_exception& ex) {
        vlog(wasm_log.warn, "invalid module (unable to parse): {}", ex);
        throw wasm_exception(ex.what(), errc::invalid_module);
    }
    bool has_wasi_main = false;

    bool has_exported_memory = false;

    for (const auto& module_export : decls.exports) {
        if (is_wasi_main(module_export)) {
            has_wasi_main = true;
            if (has_exported_memory) {
                break;
            }
        } else if (is_exported_memory(module_export)) {
            has_exported_memory = true;
            if (has_wasi_main) {
                break;
            }
        }
        co_await ss::coroutine::maybe_yield();
    }
    if (!has_wasi_main) {
        vlog(wasm_log.warn, "invalid module: missing _start function");
        throw wasm_exception(
          "invalid module: missing _start function",
          errc::invalid_module_missing_wasi);
    }
    if (!has_exported_memory) {
        vlog(wasm_log.warn, "invalid module: missing memory export");
        throw wasm_exception(
          "invalid module: missing memory export",
          errc::invalid_module_missing_wasi);
    }
    bool has_abi_check_fn = false;
    for (const auto& module_import : decls.imports) {
        if (is_transform_abi_check_fn(module_import)) {
            has_abi_check_fn = true;
        }
        if (is_invalid_sr_abi_check_fn(module_import)) {
            vlog(wasm_log.warn, "invalid module: unsupported sr abi function");
            throw wasm_exception(
              "invalid module: unsupported schema registry ABI",
              errc::invalid_module_unsupported_sr);
        }
        co_await ss::coroutine::maybe_yield();
    }
    if (!has_abi_check_fn) {
        vlog(wasm_log.warn, "invalid module: missing abi function");
        throw wasm_exception(
          "invalid module: missing transform sdk ABI check function",
          errc::invalid_module_missing_abi);
    }
}

} // namespace

std::unique_ptr<runtime> create_runtime(std::unique_ptr<schema::registry> sr) {
    return std::make_unique<wasmtime_runtime>(std::move(sr));
}

} // namespace wasm::wasmtime
