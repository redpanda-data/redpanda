/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "seastarx.h"
#include "units.h"
#include "v8_engine/internal/environment.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timer.hh>

#include <chrono>
#include <v8.h>

namespace v8_engine {

class script_exception final : public std::exception {
public:
    explicit script_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

// This class implemented container for run js code inside it.
// It stores only one function from js code and can run it.
// v8::Isolate are used like container for js env.
// It can help isolate different functions from each other
// and set up constraints for them separately.
class script {
    // Timeout for run script in first time for initialization
    static constexpr std::chrono::milliseconds _first_run_timeout_ms{500};
    static constexpr uint64_t _max_old_gen_size{10_MiB};

public:
    // Init new instance.
    // Create isolate and configure constraints for it.
    script(size_t max_heap_size_in_bytes, size_t _timeout_ms);

    script(const script& other) = delete;
    script& operator=(const script& other) = delete;
    script(script&& other) = delete;
    script& operator=(script&& other) = delete;

    // Destroy instance. Be carefull!
    // First of all run Reset for all v8::Global fileds
    // Only after that run Dispose for isolate.
    ~script();

    /// Compile js code and get function for run in future.
    ///
    /// \param function name for runnig in future
    /// \param buffer with js code for compile. TODO: think about
    /// temporary_buffer. Maybe better use iobuf and use non-seastar memory for
    /// data in v8 script. \param executor for compile script
    template<typename Executor>
    ss::future<> init(
      ss::sstring name,
      ss::temporary_buffer<char> js_code,
      Executor& executor) {
        compile_task task(*this, std::move(js_code));
        return add_future_handlers(
                 executor.submit(std::move(task), _first_run_timeout_ms))
          .then([this, name = std::move(name)] { set_function(name); });
    }

    /// Run function from js script.
    ///
    /// \param data for js script. TODO: think about temporary_buffer. Maybe
    /// better use iobuf and use non-seastar memory for data in v8 script.
    /// \param executor for run script
    template<typename Executor>
    ss::future<> run(ss::temporary_buffer<char> data, Executor& executor) {
        run_task task(*this, std::move(data));
        return add_future_handlers(
          executor.submit(std::move(task), _timeout_ms));
    }

private:
    // Must be running in executor, because it runs js code
    // in first time for init global vars and e.t.c.
    void compile_script(ss::temporary_buffer<char> js_code);

    // Init function from compiled js code.
    void set_function(std::string_view name);

    /// Run function from js code.
    /// Must be running in executor,
    /// because js function can have inf loop or smth like that.
    /// We need to controle execution time for js function

    /// \param buffer with data, which js code can read and edit.
    void run_internal(ss::temporary_buffer<char> data);

    // Throw c++ exception from v8::TryCatch
    void throw_exception_from_v8(std::string_view msg);
    void throw_exception_from_v8(
      const v8::TryCatch& try_catch, std::string_view msg);

    ss::future<> add_future_handlers(ss::future<>&& fut) {
        return fut
          .handle_exception_type([this](ss::gate_closed_exception&) {
              throw_exception_from_v8("Executor is stopped");
          })
          .handle_exception_type([this](ss::broken_semaphore&) {
              throw_exception_from_v8("Executor is stopped");
          });
    }

    struct isolate_deleter {
    public:
        void operator()(v8::Isolate* isolate) const { isolate->Dispose(); }
    };

    // Stop v8 execution if it takes too much time
    void stop_execution();
    // Cancel stop execution for reusing isolate
    void cancel_terminate_execution_for_isolate();

    std::unique_ptr<v8::Isolate, isolate_deleter> _isolate;

    v8::Global<v8::Context> _context;
    v8::Global<v8::Function> _function;

    // Script timeout
    std::chrono::milliseconds _timeout_ms;

    // This class implement task for executor. We need to add operator(),
    // cancel(), on_timeout()

    class task_for_executor {
    public:
        task_for_executor(script& script, ss::temporary_buffer<char> data)
          : _script(script)
          , _data(std::move(data)) {}

        virtual void operator()() = 0;

        void cancel() { _script.stop_execution(); }

        void on_timeout() { _script.cancel_terminate_execution_for_isolate(); }

    protected:
        script& _script;
        ss::temporary_buffer<char> _data;
    };

    friend class task_for_executor;

    class compile_task : public task_for_executor {
    public:
        compile_task(script& script, ss::temporary_buffer<char> data)
          : task_for_executor(script, std::move(data)) {}

        void operator()() override { _script.compile_script(std::move(_data)); }
    };

    class run_task : public task_for_executor {
    public:
        run_task(script& script, ss::temporary_buffer<char> data)
          : task_for_executor(script, std::move(data)) {}

        void operator()() override { _script.run_internal(std::move(_data)); }
    };
};

} // namespace v8_engine
