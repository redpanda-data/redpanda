// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "js_vm.h"

#include "quickjs.h"

#include <sys/types.h>

#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <expected>
#include <format>
#include <print>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace qjs {

value::value() noexcept
  : _ctx(nullptr)
  , _underlying(JS_UNINITIALIZED) {}

value::value(JSContext* ctx, JSValue underlying) noexcept
  : _ctx(ctx)
  , _underlying(underlying) {}

value::value(value&& other) noexcept
  : _ctx(std::exchange(other._ctx, nullptr))
  , _underlying(std::exchange(other._underlying, JS_UNINITIALIZED)) {}

value& value::operator=(value&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    JS_FreeValue(_ctx, _underlying);
    _ctx = std::exchange(other._ctx, nullptr);
    _underlying = std::exchange(other._underlying, JS_UNINITIALIZED);
    return *this;
}

value::value(const value& other) noexcept
  : _ctx(other._ctx)
  , _underlying(other.raw_dup()) {}

value& value::operator=(const value& other) noexcept {
    if (this == &other) {
        return *this;
    }
    JS_FreeValue(_ctx, _underlying);
    _ctx = other._ctx;
    _underlying = other.raw_dup();
    return *this;
}

value::~value() noexcept {
    // There is a bug with clang tidy where thinks that it's possible for _ctx
    // to be uninitialized when destructed inside of std::expected

    // NOLINTNEXTLINE
    JS_FreeValue(_ctx, _underlying);
}

value value::undefined(JSContext* ctx) { return {ctx, JS_UNDEFINED}; }
value value::object(JSContext* ctx) { return {ctx, JS_NewObject(ctx)}; }
value value::array(JSContext* ctx) { return {ctx, JS_NewArray(ctx)}; }
value value::null(JSContext* ctx) { return {ctx, JS_NULL}; }
value value::integer(JSContext* ctx, int32_t num) {
    return {ctx, JS_NewInt32(ctx, num)};
}
value value::number(JSContext* ctx, double num) {
    return {ctx, JS_NewFloat64(ctx, num)};
}
value value::boolean(JSContext* ctx, bool val) {
    return {ctx, JS_NewBool(ctx, static_cast<int>(val))};
}
std::expected<value, exception>
value::string(JSContext* ctx, std::string_view str) {
    value val = {ctx, JS_NewStringLen(ctx, str.data(), str.size())};
    if (val.is_exception()) {
        return std::unexpected(exception::current(ctx));
    }
    return val;
}
value value::error(JSContext* ctx, std::string_view msg) {
    value err = {ctx, JS_NewError(ctx)};
    auto result = value::string(ctx, msg);
    if (result) {
        std::ignore = err.set_property("message", result.value());
    } else {
        std::ignore = result;
    }
    return err;
}
std::expected<value, exception>
value::parse_json(JSContext* ctx, const std::string& str) {
    value val = {ctx, JS_ParseJSON(ctx, str.c_str(), str.size(), "")};
    if (val.is_exception()) {
        return std::unexpected(exception::current(ctx));
    }
    return val;
}
value value::array_buffer(JSContext* ctx, std::span<uint8_t> data) {
    void* opaque = nullptr;
    // NOLINTNEXTLINE(*easily-swappable-parameters)
    JSFreeArrayBufferDataFunc* func = [](JSRuntime*, void* opaque, void* ptr) {
        // Nothing to do
    };
    return {
      ctx,
      JS_NewArrayBuffer(
        ctx, data.data(), data.size(), func, opaque, /*is_shared=*/0)};
}

std::expected<value, exception>
value::array_buffer_copy(JSContext* ctx, std::span<uint8_t> data) {
    value val{ctx, JS_NewArrayBufferCopy(ctx, data.data(), data.size())};
    if (val.is_exception()) {
        return std::unexpected(exception::current(ctx));
    }
    return val;
}

value value::uint8_array(JSContext* ctx, std::span<uint8_t> data) {
    void* opaque = nullptr;
    // NOLINTNEXTLINE(*easily-swappable-parameters)
    JSFreeArrayBufferDataFunc* func = [](JSRuntime*, void* opaque, void* ptr) {
        // Nothing to do
    };

    return {
      ctx,
      JS_NewUint8Array(
        ctx, data.data(), data.size(), func, opaque, /*is_shared=*/0)};
}

// see quickjs source for detailed buffer ownership semantics
// https://github.com/quickjs-ng/quickjs/blob/da5b95dcaf372dcc206019e171a0b08983683bf5/quickjs.c#L49379-L49436
// TL;DR - With copying enabled, the array constructor registers a baked-in
// free_func to be called by the object destructor. The copy of data is made
// and mangaged internally to the quickjs runtime.
std::expected<value, exception>
value::uint8_array_copy(JSContext* ctx, std::span<uint8_t> data) {
    value val{ctx, JS_NewUint8ArrayCopy(ctx, data.data(), data.size())};
    if (val.is_exception()) {
        return std::unexpected(exception::current(ctx));
    }
    return val;
}

void value::detach_buffer() {
    assert(is_array_buffer());
    JS_DetachArrayBuffer(_ctx, _underlying);
}
std::expected<std::monostate, exception> value::detach_uint8_array() {
    assert(is_uint8_array());
    auto val = value(
      _ctx,
      JS_GetTypedArrayBuffer(_ctx, _underlying, nullptr, nullptr, nullptr));
    if (val.is_exception()) {
        return std::unexpected(exception::current(_ctx));
    }
    val.detach_buffer();
    return {};
}

std::span<uint8_t> value::array_buffer_data() const {
    assert(is_array_buffer());
    size_t size = 0;
    uint8_t* ptr = JS_GetArrayBuffer(_ctx, &size, _underlying);
    return {ptr, size};
}

std::span<uint8_t> value::uint8_array_data() const {
    assert(is_uint8_array());
    size_t size = 0;
    uint8_t* ptr = JS_GetUint8Array(_ctx, &size, _underlying);
    return {ptr, size};
}

cstring value::string_data() const {
    assert(is_string());
    size_t size = 0;
    const char* ptr = JS_ToCStringLen(_ctx, &size, _underlying);
    return {_ctx, ptr, size};
}

value value::global(JSContext* ctx) { return {ctx, JS_GetGlobalObject(ctx)}; }

value value::current_exception(JSContext* ctx) {
    return {ctx, JS_GetException(ctx)};
}
bool value::is_number() const { return JS_IsNumber(_underlying) != 0; }
bool value::is_exception() const { return JS_IsException(_underlying) != 0; }
bool value::is_error() const { return JS_IsError(_ctx, _underlying) != 0; }
bool value::is_function() const {
    return JS_IsFunction(_ctx, _underlying) != 0;
}
bool value::is_string() const { return JS_IsString(_underlying) != 0; }
bool value::is_array_buffer() const {
    return JS_IsArrayBuffer(_underlying) != 0;
}
bool value::is_uint8_array() const { return JS_IsUint8Array(_underlying) != 0; }
bool value::is_object() const { return JS_IsObject(_underlying) != 0; }
bool value::is_null() const { return JS_IsNull(_underlying) != 0; }
bool value::is_undefined() const { return JS_IsUndefined(_underlying) != 0; }
bool value::is_array() const { return JS_IsArray(_ctx, _underlying) != 0; }
JSValue value::raw() const { return _underlying; }
JSValue value::raw_dup() const { return JS_DupValue(_ctx, _underlying); }

double value::as_number() const {
    assert(is_number());
    if (JS_TAG_IS_FLOAT64(JS_VALUE_GET_TAG(_underlying))) {
        return JS_VALUE_GET_FLOAT64(_underlying);
    }
    return JS_VALUE_GET_INT(_underlying);
}

int32_t value::as_integer() const {
    auto num = as_number();
    return static_cast<int32_t>(std::lround(num));
}

std::expected<value, exception> value::call(std::span<value> values) {
    assert(is_function());
    std::vector<JSValue> raw;
    raw.reserve(values.size());
    std::ranges::transform(
      values, std::back_inserter(raw), [](const value& val) {
          return val._underlying;
      });
    const value global_this = global(_ctx);
    auto result = value(
      _ctx,
      JS_Call(
        _ctx,
        _underlying,
        global_this._underlying,
        static_cast<int>(raw.size()),
        raw.data()));
    if (result.is_exception()) {
        return std::unexpected(exception::current(_ctx));
    }
    return result;
}

value value::get_property(std::string_view key) const {
    assert(is_object());
    const JSAtom atom = JS_NewAtomLen(_ctx, key.data(), key.size());
    const JSValue val = JS_GetProperty(_ctx, _underlying, atom);
    JS_FreeAtom(_ctx, atom);
    return {_ctx, val};
}

std::expected<std::monostate, exception>
value::set_property(std::string_view key, const value& val) {
    assert(is_object());
    const JSAtom atom = JS_NewAtomLen(_ctx, key.data(), key.size());
    const int result = JS_SetProperty(_ctx, _underlying, atom, val.raw_dup());
    JS_FreeAtom(_ctx, atom);
    if (result < 0) {
        return std::unexpected(exception::current(_ctx));
    }
    return {};
}

std::expected<std::monostate, exception> value::push_back(const value& val) {
    assert(is_array());
    const size_t len = array_length();
    constexpr auto flags = JS_PROP_C_W_E; // NOLINT
    const int result = JS_DefinePropertyValueUint32(
      _ctx, _underlying, len, val.raw_dup(), flags);
    if (result < 0) {
        return std::unexpected(exception::current(_ctx));
    }
    return {};
}

value value::get_element(size_t idx) const {
    assert(is_array());
    auto raw = JS_GetPropertyUint32(_ctx, _underlying, idx);
    return {_ctx, raw};
}

size_t value::array_length() const {
    assert(is_array());
    auto prop = get_property("length");
    if (JS_VALUE_GET_TAG(prop.raw()) != JS_TAG_INT) {
        return 0;
    }
    return JS_VALUE_GET_INT(prop.raw());
}

// NOLINTNEXTLINE(*-no-recursion)
std::string value::debug_string() const {
    if (is_null()) {
        return "null";
    }
    if (is_undefined()) {
        return "undefined";
    }
    size_t size = 0;
    const char* str = JS_ToCStringLen(_ctx, &size, _underlying);
    std::string result;
    if (str != nullptr) {
        result = std::string(str, size);
        JS_FreeCString(_ctx, str);
    } else {
        result = "[exception]";
    }
    if (is_exception() || is_error()) {
        auto stack = get_property("stack");
        result += std::format("\n Stack: \n{}", stack.debug_string());
    }
    return result;
}

bool operator==(const value& lhs, const value& rhs) {
    return lhs._ctx == rhs._ctx
           && JS_IsStrictEqual(lhs._ctx, lhs.raw(), rhs.raw()) != 0;
}

exception exception::current(JSContext* ctx) {
    return {value::current_exception(ctx)};
}

exception exception::make(JSContext* ctx, std::string_view message) {
    return {value::error(ctx, message)};
}

compiled_bytecode::compiled_bytecode(
  JSContext* ctx, uint8_t* data, size_t size) noexcept
  : _ctx(ctx)
  , _data(data)
  , _size(size) {}

compiled_bytecode::compiled_bytecode(compiled_bytecode&& other) noexcept
  : _ctx(other._ctx)
  , _data(std::exchange(other._data, nullptr))
  , _size(std::exchange(other._size, 0)) {}

compiled_bytecode&
compiled_bytecode::operator=(compiled_bytecode&& other) noexcept {
    if (&other == this) {
        return *this;
    }
    _ctx = std::exchange(other._ctx, nullptr);
    _size = std::exchange(other._size, 0);
    _data = std::exchange(other._data, nullptr);
    return *this;
}

compiled_bytecode::~compiled_bytecode() noexcept {
    if (_ctx != nullptr && _data != nullptr) {
        js_free(_ctx, _data);
    }
}

compiled_bytecode::view compiled_bytecode::raw() const {
    return {_data, _size};
}

compiled_bytecode::view::iterator compiled_bytecode::begin() const {
    return raw().begin();
}

compiled_bytecode::view::iterator compiled_bytecode::end() const {
    return raw().end();
}

runtime::runtime()
  : _runtime_state(std::make_unique<runtime_state>())
  , _context_state(std::make_unique<context_state>())
  , _rt(JS_NewRuntime())
  , _ctx(JS_NewContext(_rt.get())) {
    JS_SetRuntimeOpaque(_rt.get(), _runtime_state.get());
    JS_SetContextOpaque(_ctx.get(), _context_state.get());
}

namespace {
class console {
public:
    struct config {
        FILE* info;
        FILE* warn;
    };
    explicit console(config cfg)
      : _info_stream(cfg.info)
      , _err_stream(cfg.warn) {}

    std::expected<qjs::value, qjs::exception>
    info(JSContext* /*ctx*/, std::span<qjs::value> params) {
        return log_impl(_info_stream, params);
    }

    std::expected<qjs::value, qjs::exception>
    warn(JSContext* /*ctx*/, std::span<qjs::value> params) {
        return log_impl(_err_stream, params);
    }

private:
    static std::expected<qjs::value, qjs::exception>
    log_impl(FILE* stream, std::span<qjs::value> params) {
        if (params.empty()) {
            std::println(stream, "");
            return {};
        }
        std::string buffer;
        buffer += params.front().debug_string();
        for (const auto& arg : params.subspan(1)) {
            buffer += ' ';
            buffer += arg.debug_string();
        }
        std::println(stream, "{}", buffer);
        return {};
    }

    FILE* _info_stream;
    FILE* _err_stream;
};

std::unordered_map<std::string, std::string> map_from_environ() {
    std::unordered_map<std::string, std::string> env{};
    for (char** envp = environ; *envp != nullptr;
         std::advance(envp, std::ptrdiff_t{1})) {
        // environ entries are expected to be null terminated
        const std::string entry{*envp};
        if (entry.empty()) {
            continue;
        }
        auto delim = entry.find_first_of('=');
        if (delim == std::string_view::npos) {
            continue;
        }
        auto key = entry.substr(0, delim);
        auto val = delim + 1 < entry.size() ? entry.substr(delim + 1) : "";
        env.emplace(std::move(key), std::move(val));
    }
    env.try_emplace("NODE_ENV", "production");
    return env;
}

std::expected<qjs::value, qjs::exception> env(JSContext* ctx) {
    auto native_env = map_from_environ();
    auto js_env = value::object(ctx);
    for (const auto& [k, v] : native_env) {
        auto val = value::string(ctx, v);
        if (!val.has_value()) {
            return std::unexpected(val.error());
        }
        if (auto res = js_env.set_property(k, val.value()); !res.has_value()) {
            return std::unexpected(res.error());
        }
    }
    return js_env;
}

} // namespace

std::expected<std::monostate, exception> runtime::create_builtins() {
    auto console_builder = class_builder<console>(_ctx.get(), "Console");
    console_builder.method<&console::info>("log");
    console_builder.method<&console::info>("info");
    console_builder.method<&console::info>("debug");
    console_builder.method<&console::info>("trace");
    console_builder.method<&console::warn>("warn");
    console_builder.method<&console::warn>("error");
    auto factory = console_builder.build();
    auto global_this = value::global(_ctx.get());
    auto result = global_this.set_property(
      "console",
      factory.create(std::make_unique<console>(
        console::config{.info = stdout, .warn = stderr})));

    if (!result.has_value()) {
        return result;
    }

    auto process = qjs::value::object(_ctx.get());
    auto env_obj = env(_ctx.get());
    if (!env_obj.has_value()) {
        return std::unexpected(env_obj.error());
    }
    result = process.set_property("env", env_obj.value());
    if (!result.has_value()) {
        return result;
    }
    result = global_this.set_property("process", process);
    if (!result.has_value()) {
        return result;
    }
    return global_this.set_property("self", value::global(_ctx.get()));
}

std::expected<compiled_bytecode, exception>
runtime::compile(const std::string& module_src) {
    // NOLINTBEGIN(*signed-bitwise*)
    constexpr int eval_flags = JS_EVAL_TYPE_MODULE | JS_EVAL_FLAG_COMPILE_ONLY;
    // NOLINTEND(*signed-bitwise*)
    auto result = value(
      _ctx.get(),
      JS_Eval(
        _ctx.get(),
        module_src.c_str(),
        module_src.size(),
        "file.mjs",
        eval_flags));
    if (result.is_exception()) {
        return std::unexpected(exception::current(_ctx.get()));
    }
    // NOLINTNEXTLINE(*signed-bitwise*)
    constexpr int write_flags = JS_WRITE_OBJ_BYTECODE;
    size_t output_size = 0;
    uint8_t* output = JS_WriteObject(
      _ctx.get(), &output_size, result.raw(), write_flags);
    return {compiled_bytecode(_ctx.get(), output, output_size)};
}

std::expected<std::monostate, exception>
runtime::load(compiled_bytecode::view bytecode) {
    auto* ctx = _ctx.get();
    // NOLINTNEXTLINE(*signed-bitwise*)
    constexpr int read_flags = JS_READ_OBJ_BYTECODE;
    auto read = value(
      ctx, JS_ReadObject(ctx, bytecode.data(), bytecode.size(), read_flags));
    if (read.is_exception()) {
        return std::unexpected(exception::current(ctx));
    }
    if (JS_VALUE_GET_TAG(read.raw()) == JS_TAG_MODULE) {
        if (JS_ResolveModule(ctx, read.raw()) < 0) {
            return std::unexpected(
              exception(value::error(ctx, "unable to resolve module")));
        }
        JSModuleDef* mod_def = static_cast<JSModuleDef*>(
          JS_VALUE_GET_PTR(read.raw()));
        const JSAtom name = JS_GetModuleName(ctx, mod_def);
        const char* module_name_raw = JS_AtomToCString(ctx, name);
        JS_FreeAtom(ctx, name);
        if (module_name_raw == nullptr) {
            return std::unexpected(exception::current(ctx));
        }
        std::string module_name = module_name_raw;
        JS_FreeCString(ctx, module_name_raw);
        if (!module_name.contains(":")) {
            module_name.insert(0, "file://");
        }
        value meta_obj = {ctx, JS_GetImportMeta(ctx, mod_def)};
        auto module_string_result = value::string(ctx, module_name);
        if (!module_string_result) {
            return std::unexpected(std::move(module_string_result.error()));
        }
        auto result = meta_obj.set_property(
          "url", std::move(module_string_result).value());
        if (!result) {
            return std::unexpected(std::move(result.error()));
        }
        result = meta_obj.set_property("main", value::boolean(ctx, true));
        if (!result) {
            return std::unexpected(std::move(result.error()));
        }
    }
    const JSValue js_module = read.raw_dup();
    // We need to free `read` before calling eval, as if eval fails it always
    // completely frees the input.
    read = value::null(ctx);
    auto result = value(ctx, JS_EvalFunction(ctx, js_module));
    if (result.is_exception()) {
        return std::unexpected(exception::current(ctx));
    }
    return {};
}

std::expected<std::monostate, exception>
runtime::add_module(module_builder builder) {
    return builder.build(_ctx.get());
}

module_builder::module_builder(std::string name)
  : _name(std::move(name)) {}

module_builder&
module_builder::add_function(std::string name, native_function func) {
    _exports.emplace(std::move(name), std::move(func));
    return *this;
}

module_builder&
module_builder::add_object(std::string name, object_builder obj) {
    _objects.emplace(std::move(name), std::move(obj));
    return *this;
}

std::expected<std::monostate, exception> module_builder::build(JSContext* ctx) {
    auto* ctx_state = static_cast<runtime::context_state*>(
      JS_GetContextOpaque(ctx));
    std::vector<JSCFunctionListEntry> entries;
    entries.reserve(_exports.size() + _objects.size());
    for (auto& [name, func] : _exports) {
        auto my_magic = static_cast<int16_t>(ctx_state->named_functions.size());
        runtime::named_native_function& function
          = ctx_state->named_functions.emplace_back(
            std::make_unique<std::string>(name), std::move(func));

        // NOLINTNEXTLINE(*signed-bitwise*)
        constexpr int prop_flags = JS_PROP_WRITABLE | JS_PROP_CONFIGURABLE;
        entries.push_back(JSCFunctionListEntry{
          .name = function.name->c_str(),
          .prop_flags = prop_flags,
          .def_type = JS_DEF_CFUNC,
          .magic = my_magic,
          .u = {
            .func = {
              .length = 0,
              .cproto = JS_CFUNC_generic_magic,
              .cfunc = {
                .generic_magic = [](
                                   JSContext* ctx,
                                   JSValue this_val,
                                   int argc,
                                   JSValue* argv,
                                   int magic) -> JSValue {
                    auto* ctx_state = static_cast<runtime::context_state*>(
                      JS_GetContextOpaque(ctx));
                    const native_function& func
                      = ctx_state->named_functions[magic].func;
                    std::vector<value> args;
                    args.reserve(argc);
                    for (const JSValue arg : std::span(argv, argc)) {
                        args.emplace_back(ctx, JS_DupValue(ctx, arg));
                    }
                    auto result = func(
                      ctx, value(ctx, JS_DupValue(ctx, this_val)), args);
                    if (result.has_value()) {
                        return result.value().raw_dup();
                    }
                    return JS_Throw(ctx, result.error().val.raw_dup());
                }}}}});
    }

    for (auto& [name, builder] : _objects) {
        auto obj_data = builder.build(ctx);
        if (!obj_data.has_value()) {
            return std::unexpected(obj_data.error());
        }
        const auto& obj = ctx_state->named_objects.emplace_back(
          std::make_unique<std::string>(name), std::move(obj_data).value());
        // TODO(oren): any properties? configurable, etc?
        constexpr int prop_flags = 0;
        entries.push_back(JSCFunctionListEntry{
          .name = obj.name->c_str(),
          .prop_flags = prop_flags,
          .def_type = JS_DEF_OBJECT,
          .magic = 0 /* not used  */,
          .u = {
            .prop_list = {
              .tab = obj.data.properties.data(),
              .len = static_cast<int32_t>(obj.data.properties.size()),
            }
          },
        });
    }

    JSModuleDef* mod = JS_NewCModule(
      ctx, _name.c_str() /*copied*/, [](JSContext* ctx, JSModuleDef* mod) {
          auto* ctx_state = static_cast<runtime::context_state*>(
            JS_GetContextOpaque(ctx));
          auto& func_exports = ctx_state->module_functions.at(mod);
          return JS_SetModuleExportList(
            ctx,
            mod,
            func_exports.data(),
            static_cast<int>(func_exports.size()));
      });
    if (mod == nullptr) {
        return std::unexpected(exception(value::error(
          ctx, std::format("unable to register native module {}", _name))));
    }
    JS_AddModuleExportList(
      ctx, mod, entries.data(), static_cast<int>(entries.size()));
    ctx_state->module_functions.emplace(mod, std::move(entries));
    return {};
}

object_builder& object_builder::add_string(std::string key, std::string value) {
    return add_native_value(std::move(key), std::move(value));
}
object_builder& object_builder::add_i32(std::string key, int32_t value) {
    return add_native_value(std::move(key), value);
}
object_builder& object_builder::add_i64(std::string key, int64_t value) {
    return add_native_value(std::move(key), value);
}
object_builder& object_builder::add_f64(std::string key, double value) {
    return add_native_value(std::move(key), value);
}

object_builder& object_builder::add_native_value(
  std::string key, object_builder::native_value value) {
    _properties.emplace(std::move(key), std::move(value));
    return *this;
}

std::expected<object_export_data, exception>
object_builder::build(JSContext* ctx) {
    std::vector<std::unique_ptr<std::string>> pnames;
    pnames.reserve(_properties.size());
    std::vector<JSCFunctionListEntry> props;
    props.reserve(_properties.size());
    std::vector<std::unique_ptr<std::string>> strs;
    for (const auto& [pname, pval] : _properties) {
        pnames.push_back(std::make_unique<std::string>(pname));
        if (const auto* arg = std::get_if<std::string>(&pval)) {
            strs.push_back(std::make_unique<std::string>(*arg));
            props.push_back(JSCFunctionListEntry{
              .name = pnames.back()->c_str(),
              .prop_flags = 0,
              .def_type = JS_DEF_PROP_STRING,
              .magic = 0, /* not used */
              .u = {.str = strs.back()->c_str()},
            });
        } else if (const auto* arg = std::get_if<int32_t>(&pval)) {
            props.push_back(JSCFunctionListEntry{
              .name = pnames.back()->c_str(),
              .prop_flags = 0,
              .def_type = JS_DEF_PROP_INT32,
              .magic = 0, /* not used */
              .u = {.i32 = static_cast<int32_t>(*arg)},
            });
        } else if (const auto* arg = std::get_if<int64_t>(&pval)) {
            props.push_back(JSCFunctionListEntry{
              .name = pnames.back()->c_str(),
              .prop_flags = 0,
              .def_type = JS_DEF_PROP_INT64,
              .magic = 0, /* not used */
              .u = {.i64 = static_cast<int64_t>(*arg)},
            });
        } else if (const auto* arg = std::get_if<double>(&pval)) {
            props.push_back(JSCFunctionListEntry{
              .name = pnames.back()->c_str(),
              .prop_flags = 0,
              .def_type = JS_DEF_PROP_DOUBLE,
              .magic = 0, /* not used */
              .u = {.f64 = static_cast<double>(*arg)},
            });
        } else {
            return std::unexpected(qjs::exception::make(
              ctx,
              std::format(
                "object builder failed - bad type for '{}'", *pnames.back())));
        }
    }

    return object_export_data{
      .property_names = std::move(pnames),
      .str_values = std::move(strs),
      .properties = std::move(props),
    };
}

cstring::cstring(JSContext* context, const char* ptr, size_t size)
  : context(context)
  , ptr(ptr)
  , size(size) {}
cstring::~cstring() {
    if (ptr != nullptr) {
        // NOLINTNEXTLINE
        js_free(context, const_cast<char*>(ptr));
    }
}
cstring::cstring(cstring&& other) noexcept
  : context(other.context)
  , ptr(std::exchange(other.ptr, nullptr))
  , size(other.size) {}

std::string_view cstring::view() const { return {ptr, size}; }

} // namespace qjs
