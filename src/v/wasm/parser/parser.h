/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <limits>
#include <variant>
#include <vector>

namespace wasm {
/**
 * Fundamental types that exist in the WebAssembly core v2 specification.
 */
enum class val_type : uint8_t {
    i32 = 0x7f,
    i64 = 0x7e,
    f32 = 0x7d,
    f64 = 0x7c,
    v128 = 0x7b,
    funcref = 0x70,
    externref = 0x6f,
};

/**
 * A signature of a function within WebAssembly (yes they support multiple
 * return types).
 */
struct function_signature {
    std::vector<val_type> parameters;
    std::vector<val_type> results;

    bool operator==(const function_signature&) const = default;
    friend std::ostream& operator<<(std::ostream&, const function_signature&);
};
} // namespace wasm

namespace wasm::parser {

namespace declaration {

/**
 * Bounds on a component (table/memory) during runtime.
 */
struct limits {
    uint32_t min = 0;
    uint32_t max = std::numeric_limits<uint32_t>::max();

    bool operator==(const limits&) const = default;
    friend std::ostream& operator<<(std::ostream&, const limits&);
};

/**
 * A declaration of a function, with a signature.
 */
struct function {
    function_signature signature;

    bool operator==(const function&) const = default;
    friend std::ostream& operator<<(std::ostream&, const function&);
};

/**
 * A declaration of a table it's types and bounds for it during runtime.
 */
struct table {
    val_type reftype; // only ever funcref or externref
    limits limits;

    bool operator==(const table&) const = default;
    friend std::ostream& operator<<(std::ostream&, const table&);
};

/**
 * A declaration of linear memory and bounds for it during runtime.
 */
struct memory {
    limits limits;

    bool operator==(const memory&) const = default;
    friend std::ostream& operator<<(std::ostream&, const memory&);
};

/**
 * A declaration of a global variable and if it is mutable or not.
 */
struct global {
    val_type valtype;
    bool is_mutable;

    bool operator==(const global&) const = default;
    friend std::ostream& operator<<(std::ostream&, const global&);
};

} // namespace declaration

/**
 * The type of import: functions/tables/memories/globals.
 */
using import_description = std::variant<
  declaration::function,
  declaration::table,
  declaration::memory,
  declaration::global>;

/**
 * An import in the WebAssembly v2 specification, which is namespaced by import
 * module and the name of the item to import.
 */
struct module_import {
    ss::sstring module_name;
    ss::sstring item_name;
    import_description description;

    bool operator==(const module_import&) const = default;
    friend std::ostream& operator<<(std::ostream&, const module_import&);
};

/**
 * The type of export: functions/tables/memories/globals.
 */
using export_description = std::variant<
  declaration::function,
  declaration::table,
  declaration::memory,
  declaration::global>;

/**
 * An export in the WebAssembly v2 specification, which is a name and the item
 * being exported.
 */
struct module_export {
    ss::sstring item_name;
    export_description description;

    bool operator==(const module_export&) const = default;
    friend std::ostream& operator<<(std::ostream&, const module_export&);
};

/**
 * The declarations of a WebAssembly module.
 */
struct module_declarations {
    chunked_vector<module_export> exports;
    chunked_vector<module_import> imports;

    bool operator==(const module_declarations&) const = default;
    friend std::ostream& operator<<(std::ostream&, const module_declarations&);
};

/**
 * An exception thrown for an invalid wasm module.
 */
class parse_exception : public std::exception {
public:
    explicit parse_exception(std::string msg)
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    std::string _msg;
};

/**
 * An exception thrown when one of our parsing limits would be exceeded.
 */
class module_too_large_exception : public parse_exception {
public:
    explicit module_too_large_exception(std::string msg)
      : parse_exception(std::move(msg)) {}
};

/**
 * Given a WebAssembly module in binary form, extract the imports and exports
 * the module declares.
 *
 * NOTE: We don't need a full blown wasm parser because we only validate that a
 * module that is being deployed supports our ABI.
 */
ss::future<module_declarations> extract_declarations(iobuf binary_module);

} // namespace wasm::parser
