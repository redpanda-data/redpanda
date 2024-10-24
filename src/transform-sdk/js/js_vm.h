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

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <functional>
#include <memory>
#include <print>
#include <quickjs.h>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

template<typename T, auto fn>
struct deleter {
    void operator()(T* ptr) {
        fn(ptr); // NOLINT
    }
};
template<typename T, auto fn>
using handle = std::unique_ptr<T, deleter<T, fn>>;

/**
 * Wrappers for the QuickJS JavaScript runtime.
 *
 * QuickJS API's have some quirks and are not currently well documented.
 * Hopefully this documentation is sufficent, otherwise the tests should give an
 * overview of functionality (as well as giving usage examples).
 *
 */
namespace qjs {

struct exception;
class value;

/**
 * A small string wrapper that was allocated from C.
 */
struct cstring {
public:
    cstring(JSContext* context, const char* ptr, size_t size);
    cstring(const cstring&) = delete;
    cstring(cstring&& other) noexcept;
    cstring& operator=(const cstring&) = delete;
    cstring& operator=(cstring&&) = delete;
    ~cstring();

    [[nodiscard]] std::string_view view() const;

private:
    JSContext* context;
    const char* ptr;
    size_t size;
};

/**
 * A JavaScript function natively defined in C++
 */
using native_function = std::function<std::expected<value, exception>(
  JSContext* ctx, value this_val, std::span<value> args)>;

/**
 * A class function natively defined in C++
 *
 * Used for custom (native) classes and defining functions for said classes.
 */
template<typename T>
using native_class_function_ptr = std::expected<value, exception> (T::*)(
  JSContext* ctx, std::span<value> args);

/**
 * A value within Javascript that is ref counted.
 *
 * Due to the reference counting, we always need to keep a pointer to the
 * original JSContext the value is attached to. It's not valid to mix values
 * from different contexts at the moment (although QuickJS supports it for
 * primative types).
 */
class value {
public:
    value() noexcept;
    value(JSContext* ctx, JSValue underlying) noexcept;
    value(const value&) noexcept;
    value& operator=(const value&) noexcept;
    value(value&& other) noexcept;
    value& operator=(value&& other) noexcept;
    ~value() noexcept;

    /** Create a null value. */
    static value undefined(JSContext* ctx);
    /** Create an empty plain object value. */
    static value object(JSContext* ctx);
    /** Create an empty array value. */
    static value array(JSContext* ctx);
    /** Create a null value. */
    static value null(JSContext* ctx);
    /** Create an int value. */
    static value integer(JSContext* ctx, int32_t);
    /** Create a double value. */
    static value number(JSContext* ctx, double);
    /** Create a bool value. */
    static value boolean(JSContext* ctx, bool);
    /** Create a string value (copy). */
    static std::expected<value, exception>
    string(JSContext* ctx, std::string_view);
    /** Create an error class */
    static value error(JSContext* ctx, std::string_view);
    /**
     * Parse a value from JSON.
     *
     * NOTE: This must be `std::string` because quickjs requires a trailing null
     * byte.
     */
    static std::expected<value, exception>
    parse_json(JSContext* ctx, const std::string&);
    /**
     * Create an array buffer value (view - no copies).
     *
     * This should be used with care and one should always call `detach_buffer`
     * when `data` is about to go out of scope to prevent the javascript side
     * from accessing free'd memory.
     *
     */
    static value array_buffer(JSContext* ctx, std::span<uint8_t> data);

    /** Create a typed array buffer value (copy) */
    static std::expected<value, exception>
    array_buffer_copy(JSContext* ctx, std::span<uint8_t> data);

    /**
     * Create an array buffer value (view - no copies).
     *
     * This should be used with care and one should always call
     * `detach_uint8_array` when `data` is about to go out of scope to prevent
     * the javascript side from accessing free'd memory.
     *
     */
    static value uint8_array(JSContext* ctx, std::span<uint8_t> data);

    /** Create a typed uint8 array value (copy) */
    static std::expected<value, exception>
    uint8_array_copy(JSContext* ctx, std::span<uint8_t> data);

    /**
     * Free the underlying memory to an array buffer.
     */
    void detach_buffer();

    /**
     * Free the underlying memory to an uint8 array.
     */
    [[nodiscard]] std::expected<std::monostate, exception> detach_uint8_array();

    /**
     * Return the data for an underlying array buffer object.
     *
     * Returns an empty span if not an array buffer or if it's detached.
     */
    [[nodiscard]] std::span<uint8_t> array_buffer_data() const;

    /**
     * Return the data for an underlying uint8 array object.
     *
     * Returns an empty span if not an array buffer or if it's detached.
     */
    [[nodiscard]] std::span<uint8_t> uint8_array_data() const;

    /**
     * Return the data for an underlying string object.
     *
     * Returns an empty string_view if not a string.
     */
    [[nodiscard]] cstring string_data() const;

    /**
     * Get the global object for this context.
     */
    static value global(JSContext* ctx);

    /**
     * Get the current exception (when an exception is thrown and detected via
     * is_exception).
     */
    static value current_exception(JSContext* ctx);

    /** Is this value a integer? */
    [[nodiscard]] bool is_number() const;
    /** Is this value an exception? */
    [[nodiscard]] bool is_exception() const;
    /** Is this value an error? */
    [[nodiscard]] bool is_error() const;
    /** Is this value a function? */
    [[nodiscard]] bool is_function() const;
    /** Is this value a string? */
    [[nodiscard]] bool is_string() const;
    /** Check if this is an array buffer */
    [[nodiscard]] bool is_array_buffer() const;
    /** Check if this is an uint8 array */
    [[nodiscard]] bool is_uint8_array() const;
    /** Check if this is an object */
    [[nodiscard]] bool is_object() const;
    /** Check if this is null */
    [[nodiscard]] bool is_null() const;
    /** Check if this is undefined */
    [[nodiscard]] bool is_undefined() const;
    /** Check if this is an array */
    [[nodiscard]] bool is_array() const;

    /** Get the number from the object. */
    [[nodiscard]] double as_number() const;

    /** Get an integer from the object. Rounding. */
    [[nodiscard]] int32_t as_integer() const;

    /**
     * Return a reference to the raw JSValue without incrementing the ref
     * count.
     *
     * This is a low level API and mostly exposed for interop, it shouldn't be
     * needed by consumers of this library.
     */
    [[nodiscard]] JSValue raw() const;
    /**
     * Return a reference to the raw JSValue, incrementing the ref
     * count.
     *
     * This is a low level API and mostly exposed for interop, it shouldn't be
     * needed by consumers of this library.
     */
    [[nodiscard]] JSValue raw_dup() const;

    /**
     * Call a javascript function with the given arguments.
     */
    [[nodiscard]] std::expected<value, exception> call(std::span<value> values);

    /**
     * Access a property on a javascript object, undefined if not set.
     *
     * Example: `obj["foo"]`
     */
    [[nodiscard]] value get_property(std::string_view) const;

    /**
     * Set a property on a javascript object.
     *
     * Example: `obj["foo"] = 5`
     */
    [[nodiscard]] std::expected<std::monostate, exception>
    set_property(std::string_view, const value&);

    /**
     * Append a property to a JavaScript array.
     *
     * Example: `arr.push(5)`
     */
    [[nodiscard]] std::expected<std::monostate, exception>
    push_back(const value&);

    /**
     * Get a value at an index within a JavaScript array.
     *
     * Example: `arr[5]`
     */
    [[nodiscard]] value get_element(size_t) const;

    /**
     * Get the length of a JavaScript array.
     */
    [[nodiscard]] size_t array_length() const;

    /**
     * Call toString() on the Javascript value and return the resulting value as
     * a native string.
     */
    [[nodiscard]] std::string debug_string() const;

    friend bool operator==(const value&, const value&);

private:
    JSContext* _ctx;
    JSValue _underlying;
};

/**
 * An exception within the JavaScript runtime.
 *
 * JavaScript exceptions can be any arbitrary value - this struct mostly exists
 * to provide clarity and typesafety for exceptions.
 */
struct exception {
    value val;

    /** Create an exception from the currently thrown one. */
    static exception current(JSContext* ctx);

    static exception make(JSContext* ctx, std::string_view message);
};

struct object_export_data {
    std::vector<std::unique_ptr<std::string>> property_names;
    std::vector<std::unique_ptr<std::string>> str_values;
    std::vector<JSCFunctionListEntry> properties;
};

/**
 * A builder of exportable objects.
 *
 * Allows building up singleton JavaScript objects for export in a
 * native module. Currently supports only primitive types and strings,
 * but could be extended to support native functions or subobjects as well.
 *
 * One use case is for constructing an 'enum', which JS doesn't natively
 * natively support. An enum defined in TypeScript would be transpiled
 * to a JavaScript object; this interface allows us to place such an enum in
 * our typings without having to rely on the TS compiler to generate the
 * corresponding object in plain JS.
 *
 */
class object_builder {
public:
    /**
     * Create an object for module export
     */
    explicit object_builder() = default;
    object_builder(const object_builder&) = delete;
    object_builder& operator=(const object_builder&) = delete;
    object_builder(object_builder&&) = default;
    object_builder& operator=(object_builder&&) = default;
    ~object_builder() = default;

    object_builder& add_string(std::string key, std::string);
    object_builder& add_i32(std::string key, int32_t);
    object_builder& add_i64(std::string key, int64_t);
    object_builder& add_f64(std::string key, double);

    std::expected<object_export_data, exception> build(JSContext* ctx);

private:
    using native_value = std::variant<std::string, int32_t, int64_t, double>;
    object_builder& add_native_value(std::string, native_value);

    std::unordered_map<std::string, native_value> _properties;
};

/**
 * A builder of native modules.
 *
 * This class allows embedded natively (C++) defined APIs and exposing them as
 * module imports to loaded JavaScript modules.
 *
 * Currently this only supports defining functions, because that is all that is
 * needed.
 */
class module_builder {
public:
    /**
     * Create a module with the given name.
     */
    explicit module_builder(std::string name);
    module_builder(const module_builder&) = delete;
    module_builder& operator=(const module_builder&) = delete;
    module_builder(module_builder&&) = default;
    module_builder& operator=(module_builder&&) = default;
    ~module_builder() = default;

    /**
     * Add an exported function to this module.
     */
    module_builder& add_function(std::string name, native_function);

    module_builder& add_object(std::string name, object_builder obj);

    /** Build this module and make it available in this context. */
    std::expected<std::monostate, exception> build(JSContext* ctx);

private:
    std::string _name;

    std::unordered_map<std::string, native_function> _exports;
    std::unordered_map<std::string, object_builder> _objects;
};

/**
 * Precompiled bytecode for the QuickJS runtime. This is serializable and can be
 * persisted to disk (or embedded in a Wasm binary).
 */
class compiled_bytecode {
public:
    using view = std::span<uint8_t>;

    /**
     * Takes ownership of `data`.
     */
    compiled_bytecode(JSContext*, uint8_t* data, size_t size) noexcept;
    compiled_bytecode(const compiled_bytecode&) = delete;
    compiled_bytecode& operator=(const compiled_bytecode&) = delete;
    compiled_bytecode(compiled_bytecode&& other) noexcept;
    compiled_bytecode& operator=(compiled_bytecode&& other) noexcept;
    ~compiled_bytecode() noexcept;

    [[nodiscard]] view raw() const;
    [[nodiscard]] view::iterator begin() const;
    [[nodiscard]] view::iterator end() const;

private:
    JSContext* _ctx;
    uint8_t* _data;
    size_t _size;
};

/**
 * A single runtime/context for the JS engine.
 *
 * Technically QuickJS supports multiple contexts for a single runtime (like a
 * browser supports multiple origins) but we don't use any of that
 * functionality.
 */
class runtime {
public:
    runtime();
    runtime(const runtime&) = delete;
    runtime& operator=(const runtime&) = delete;
    runtime(runtime&&) = default;
    runtime& operator=(runtime&&) = default;
    ~runtime() = default;

    /**
     * Compile javascript into qjs bytecode.
     */
    std::expected<compiled_bytecode, exception>
    compile(const std::string& module_src);

    /**
     * Load previously compiled bytecode into the runtime.
     */
    std::expected<std::monostate, exception>
    load(compiled_bytecode::view bytecode);

    /** Add a native module that is accessible to javascript. */
    std::expected<std::monostate, exception> add_module(module_builder);

    /** Add builtin functions like console.log */
    std::expected<std::monostate, exception> create_builtins();

    [[nodiscard]] JSContext* context() const { return _ctx.get(); }

private:
    struct runtime_state {
        struct class_info {
            JSClassID class_id;
            std::vector<JSCFunctionListEntry> entries;
        };
        // It's important this gives pointer stability.
        std::unordered_map<std::string, class_info> classes;
    };

    struct named_native_function {
        std::unique_ptr<std::string> name;
        native_function func;
    };

    struct named_object_data {
        std::unique_ptr<std::string> name;
        object_export_data data;
    };

    struct context_state {
        std::vector<named_native_function> named_functions;
        std::vector<named_object_data> named_objects;
        // For each module, the functions that are defined for it.
        std::unordered_map<JSModuleDef*, std::vector<JSCFunctionListEntry>>
          module_functions;
    };

    template<typename>
    friend class class_builder;
    friend class module_builder;

    std::unique_ptr<runtime_state> _runtime_state;
    std::unique_ptr<context_state> _context_state;
    handle<JSRuntime, JS_FreeRuntime> _rt;
    handle<JSContext, JS_FreeContext> _ctx;
};

/**
 * A helper to create instances of custom (defined in C++) JavaScript classes.
 */
template<typename T>
class class_factory {
public:
    class_factory(const class_factory&) = delete;
    class_factory(class_factory&&) = delete;
    class_factory& operator=(const class_factory&) = delete;
    class_factory& operator=(class_factory&&) = delete;
    ~class_factory() = default;

    /**
     * Create a class wrapping and taking ownership of `ptr`.
     *
     * This allows grabbing a pointer to `ptr` to have access to the native
     * along with the returned JavaScript value.
     *
     * Once the returned value is destroyed, then `ptr` will be deleted.
     */
    value create(std::unique_ptr<T> ptr) {
        value result = {
          _ctx, JS_CallConstructor(_ctx, _constructor.raw(), 0, nullptr)};
        if (result.is_exception()) [[unlikely]] {
            std::println(
              "constructor error: {}",
              exception::current(_ctx).val.debug_string());
            std::abort();
        }
        assert(JS_GetClassID(result.raw()) == _class_id);
        JS_SetOpaque(result.raw(), ptr.release());
        return result;
    }

    /**
     * Get the raw T associated with this value.
     *
     * If the value is not an instance of this class then `nullptr` is returned.
     */
    T* get_opaque(const qjs::value& val) {
        return static_cast<T*>(JS_GetOpaque(val.raw(), _class_id));
    }

private:
    class_factory(JSContext* ctx, value constructor, JSClassID class_id)
      : _ctx(ctx)
      , _constructor(std::move(constructor))
      , _class_id(class_id) {}

    template<typename>
    friend class class_builder;

    JSContext* _ctx;
    value _constructor;
    JSClassID _class_id;
};

/**
 * Allows building and exposing new classes from C++ into JavaScript.
 *
 * Once built/defined, a class_factory can be used to construct new instances of
 * the class.
 *
 * See the tests for usage.
 */
template<typename T>
class class_builder {
public:
    /**
     * Create a new class builder for a class with the given name.
     *
     * @param class_name should be a constant null-terminated string.
     */
    explicit class_builder(JSContext* ctx, const char* class_name)
      : _class_name(class_name)
      , _ctx(ctx) {}

    /**
     * Expose FuncPtr native method on T to JavaScript under the given
     * `func_name`.
     *
     * @param func_name should be a constant null-terminated string.
     */
    template<native_class_function_ptr<T> FuncPtr>
    class_builder& method(const char* func_name) {
        register_class_function<FuncPtr>(func_name);
        return *this;
    }

    /**
     * Create a class factory from this classes' definition.
     */
    class_factory<T> build() {
        auto* runtime = JS_GetRuntime(_ctx);
        JSClassID class_id = JS_INVALID_CLASS_ID;
        JS_NewClassID(runtime, &class_id);
        JSClassDef def = {
          .class_name = _class_name,
          .finalizer = cleanup_class_state,
          .gc_mark = nullptr,
          .call = nullptr,
          .exotic = nullptr,
        };
        auto* state = static_cast<runtime::runtime_state*>(
          JS_GetRuntimeOpaque(runtime));
        for (auto& entry : _entries) {
            entry.magic = static_cast<int16_t>(class_id);
        }
        auto [it, inserted] = state->classes.emplace(
          _class_name,
          runtime::runtime_state::class_info{
            .class_id = class_id,
            .entries = std::move(_entries),
          });
        JS_NewClass(runtime, class_id, &def);
        value class_proto = {_ctx, JS_NewObject(_ctx)};
        JS_SetPropertyFunctionList(
          _ctx,
          class_proto.raw(),
          it->second.entries.data(),
          static_cast<int>(it->second.entries.size()));
        value class_constructor_func = {
          _ctx,
          JS_NewCFunctionMagic(
            _ctx,
            constructor,
            _class_name,
            0,
            JS_CFUNC_constructor_magic,
            static_cast<int>(class_id)),
        };
        JS_SetConstructor(
          _ctx, class_constructor_func.raw(), class_proto.raw());
        JS_SetClassProto(_ctx, class_id, class_proto.raw_dup());
        return class_factory<T>(_ctx, class_constructor_func, class_id);
    }

private:
    template<native_class_function_ptr<T> FuncPtr>
    void register_class_function(const char* func_name) {
        _entries.push_back(JSCFunctionListEntry{
              .name = func_name,
              // NOLINTNEXTLINE(*-signed-bitwise)
              .prop_flags = JS_PROP_WRITABLE | JS_PROP_CONFIGURABLE,
              .def_type = JS_DEF_CFUNC,
              .magic = 0, // Will be set to class ID later
              .u = {
                .func = {
                  .length = 0,
                  .cproto = JS_CFUNC_generic_magic,
                  .cfunc = {
                    .generic_magic = invoke_class_func_ptr<FuncPtr>,
                  },
                },
              },
            });
    }

    template<native_class_function_ptr<T> FuncPtr>
    static JSValue invoke_class_func_ptr(
      JSContext* ctx, JSValue this_val, int argc, JSValue* argv, int magic) {
        JSClassID class_id = magic;
        std::vector<value> args;
        args.reserve(argc);
        for (JSValue arg : std::span(argv, argc)) {
            args.emplace_back(ctx, JS_DupValue(ctx, arg));
        }
        void* opaque = JS_GetOpaque2(ctx, this_val, class_id);
        if (opaque == nullptr) {
            // NOLINTNEXTLINE(*-vararg)
            return JS_ThrowTypeError(
              ctx, "missing opaque data for: %d", class_id);
        }
        T* obj = static_cast<T*>(opaque);
        auto result = (obj->*FuncPtr)(ctx, args);
        if (result.has_value()) {
            return result.value().raw_dup();
        }
        return JS_Throw(ctx, result.error().val.raw_dup());
    }

    static void cleanup_class_state(JSRuntime* /*runtime*/, JSValue val) {
        // NOLINTNEXTLINE(*-owning-memory)
        delete static_cast<T*>(JS_GetOpaque(val, JS_GetClassID(val)));
    }

    static JSValue constructor(
      JSContext* ctx,
      JSValue new_target,
      int /*paramc*/,
      JSValue* /*paramv*/,
      int magic) {
        JSClassID class_id = magic;
        auto proto = value(
          ctx, JS_GetPropertyStr(ctx, new_target, "prototype"));
        if (proto.is_exception()) {
            return JS_EXCEPTION;
        }
        value obj = value(
          ctx, JS_NewObjectProtoClass(ctx, proto.raw(), class_id));
        if (obj.is_exception()) {
            return JS_EXCEPTION;
        }
        return obj.raw_dup();
    }

    friend class runtime;

    const char* _class_name;
    JSContext* _ctx;
    std::vector<JSCFunctionListEntry> _entries;
};

} // namespace qjs
