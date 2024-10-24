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

#include "wasm/parser/parser.h"

#include "base/units.h"
#include "bytes/iobuf_parser.h"
#include "strings/utf8.h"
#include "wasm/parser/leb128.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/algorithm/container.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <ranges>
#include <stdexcept>
#include <variant>

namespace wasm {

namespace {
std::string_view to_string(const wasm::val_type& vt) {
    switch (vt) {
    case wasm::val_type::i32:
        return "i32";
    case wasm::val_type::i64:
        return "i64";
    case wasm::val_type::f32:
        return "f32";
    case wasm::val_type::f64:
        return "f64";
    case wasm::val_type::v128:
        return "v128";
    case wasm::val_type::funcref:
        return "funcref";
    case wasm::val_type::externref:
        return "externref";
    }
    return "unknown";
}
} // namespace

std::ostream& operator<<(std::ostream& os, const function_signature& sig) {
    fmt::print(
      os,
      "{{params:[{}],results:[{}]}}",
      fmt::join(sig.parameters | std::views::transform(to_string), ","),
      fmt::join(sig.results | std::views::transform(to_string), ","));
    return os;
}

} // namespace wasm

namespace wasm::parser {

namespace declaration {
std::ostream& operator<<(std::ostream& os, const limits& l) {
    fmt::print(os, "{{min:{},max:{}}}", l.min, l.max);
    return os;
}
std::ostream& operator<<(std::ostream& os, const function& f) {
    fmt::print(os, "{{signature:{}}}", f.signature);
    return os;
}
std::ostream& operator<<(std::ostream& os, const table& t) {
    fmt::print(os, "{{reftype:{},limits:{}}}", to_string(t.reftype), t.limits);
    return os;
}
std::ostream& operator<<(std::ostream& os, const memory& m) {
    fmt::print(os, "{{limits:{}}}", m.limits);
    return os;
}
std::ostream& operator<<(std::ostream& os, const global& m) {
    fmt::print(os, "{{valtype:{},mut:{}}}", to_string(m.valtype), m.is_mutable);
    return os;
}

} // namespace declaration

std::ostream& operator<<(std::ostream& os, const module_import& m_import) {
    std::visit(
      [&](auto desc) {
          fmt::print(
            os,
            "{{name:{}/{},desc:{}}}",
            m_import.module_name,
            m_import.item_name,
            desc);
      },
      m_import.description);
    return os;
}
std::ostream& operator<<(std::ostream& os, const module_export& m_export) {
    std::visit(
      [&](auto desc) {
          fmt::print(os, "{{name:{},desc:{}}}", m_export.item_name, desc);
      },
      m_export.description);
    return os;
}

std::ostream& operator<<(std::ostream& os, const module_declarations& m_decls) {
    fmt::print(
      os, "{{imports:{},exports:{}}}", m_decls.imports, m_decls.exports);
    return os;
}

namespace {
constexpr size_t max_vector_bytes = 1_MiB;
constexpr size_t max_functions
  = max_vector_bytes
    / std::max(sizeof(function_signature), sizeof(declaration::function));
constexpr size_t max_items = max_vector_bytes
                             / std::max({
                               sizeof(declaration::memory),
                               sizeof(declaration::table),
                               sizeof(declaration::global),
                             });
constexpr size_t max_function_params = 128;
constexpr size_t max_function_results = 64;
constexpr size_t max_imports = max_vector_bytes / sizeof(module_import);
constexpr size_t max_exports = max_vector_bytes / sizeof(module_export);
constexpr size_t max_name_length = 512;

template<typename Vector>
void validate_in_range(std::string_view msg, size_t idx, const Vector& v) {
    if (idx >= v.size()) {
        throw parse_exception(
          fmt::format("{} out of range - {} ∉ [0, {})", msg, idx, v.size()));
    }
}

val_type parse_val_type(iobuf_parser_base* parser) {
    auto type_id = parser->consume_type<uint8_t>();
    switch (type_id) {
    case uint8_t(val_type::i32):
    case uint8_t(val_type::i64):
    case uint8_t(val_type::f32):
    case uint8_t(val_type::f64):
    case uint8_t(val_type::funcref):
    case uint8_t(val_type::externref):
        return val_type(type_id);
    default:
        throw parse_exception(fmt::format("unknown valtype: {}", type_id));
    }
}

val_type parse_ref_type(iobuf_parser_base* parser) {
    auto reftype = parse_val_type(parser);
    if (reftype != val_type::externref && reftype != val_type::funcref) {
        throw parse_exception(
          fmt::format("invalid reftype: {}", uint8_t(reftype)));
    }
    return reftype;
}

ss::sstring parse_name(iobuf_parser_base* parser) {
    auto str_len = leb128::decode<uint32_t>(parser);
    if (str_len > max_name_length) {
        throw parse_exception(fmt::format("name too long: {}", str_len));
    }
    auto b = parser->read_string(str_len);
    if (!is_valid_utf8(b)) {
        throw parse_exception("name invalid utf8");
    }
    return {b.begin(), b.end()};
}

std::vector<val_type>
parse_signature_types(iobuf_parser_base* parser, size_t max) {
    auto vector_size = leb128::decode<uint32_t>(parser);
    if (vector_size > max) {
        throw module_too_large_exception(
          fmt::format("too many types to function: {}", vector_size));
    }
    std::vector<val_type> result_type;
    result_type.reserve(vector_size);
    for (uint32_t i = 0; i < vector_size; ++i) {
        result_type.push_back(parse_val_type(parser));
    }
    return result_type;
}

function_signature parse_signature(iobuf_parser_base* parser) {
    auto magic = parser->consume_type<uint8_t>();
    constexpr uint8_t signature_start_magic_byte = 0x60;
    if (magic != signature_start_magic_byte) {
        throw parse_exception(
          fmt::format("function type magic mismatch: {}", magic));
    }
    auto parameter_types = parse_signature_types(parser, max_function_params);
    auto result_types = parse_signature_types(parser, max_function_results);
    return {
      .parameters = std::move(parameter_types),
      .results = std::move(result_types)};
}

declaration::limits parse_limits(iobuf_parser_base* parser) {
    if (parser->read_bool()) {
        auto min = leb128::decode<uint32_t>(parser);
        auto max = leb128::decode<uint32_t>(parser);
        return {.min = min, .max = max};
    } else {
        auto min = leb128::decode<uint32_t>(parser);
        return {.min = min, .max = std::numeric_limits<uint32_t>::max()};
    }
}

declaration::table parse_table_type(iobuf_parser_base* parser) {
    auto reftype = parse_ref_type(parser);
    auto limits = parse_limits(parser);
    return {.reftype = reftype, .limits = limits};
}

declaration::memory parse_memory_type(iobuf_parser_base* parser) {
    return {.limits = parse_limits(parser)};
}

declaration::global parse_global_type(iobuf_parser_base* parser) {
    auto valtype = parse_val_type(parser);
    auto mut = parser->read_bool();
    return {.valtype = valtype, .is_mutable = bool(mut)};
}

void skip_global_constexpr(iobuf_parser_base* parser) {
    enum class op : uint8_t {
        global_get = 0x23,
        i32_const = 0x41,
        i64_const = 0x42,
        f32_const = 0x43,
        f64_const = 0x44,
        ref_null = 0xD0,
    };
    auto opcode = parser->consume_type<op>();
    switch (opcode) {
    case op::global_get:
    case op::i32_const:
        std::ignore = leb128::decode<uint32_t>(parser);
        break;
    case op::i64_const:
        std::ignore = leb128::decode<uint64_t>(parser);
        break;
    case op::f32_const:
        std::ignore = parser->consume_type<float>();
        break;
    case op::f64_const:
        std::ignore = parser->consume_type<double>();
        break;
    case op::ref_null:
        std::ignore = parse_ref_type(parser);
        break;
    default:
        throw parse_exception(fmt::format(
          "unimplemented global opcode: {}", static_cast<uint8_t>(opcode)));
    }
    auto end = parser->consume_type<uint8_t>();
    constexpr uint8_t end_expression_marker = 0x0B;
    if (end != end_expression_marker) {
        throw parse_exception(
          fmt::format("expected end of global initalizer, got: {}", end));
    }
}

class module_extractor {
public:
    explicit module_extractor(iobuf_parser_base* parser)
      : _parser(parser) {}

    // Parse a WebAssembly binary module.
    //
    // This specific function mostly checks the magic bytes and version, then
    // parses each section of the module.
    ss::future<module_declarations> parse() {
        bytes magic = _parser->read_bytes(4);
        const bytes magic_bytes = bytes({0x00, 0x61, 0x73, 0x6D});
        if (magic != magic_bytes) {
            throw parse_exception(
              fmt::format("magic bytes incorrect: {}", magic));
        }
        auto version = _parser->consume_type<int32_t>();
        if (version != 1) {
            throw parse_exception("unsupported wasm version");
        }
        while (_parser->bytes_left() > 0) {
            parse_one_section();
            co_await ss::coroutine::maybe_yield();
        }
        co_return module_declarations{
          .exports = std::move(_exports),
          .imports = std::move(_imports),
        };
    }

private:
    // The main loop of parsing functions.
    //
    // At a high level from the spec. The sections are as follows and parsed in
    // order:
    // - type signatures
    // - imports
    // - function forward declarations
    // - tables
    // - memories
    // - globals
    // - exports
    // - start function
    // - elements (initializer code for tables)
    // - data counts
    // - code (the bodies of functions that were forward declared)
    // - data (initializer code for memories)
    //
    // Around any of these sections can be a custom section.
    //
    // We currently ignore any of the sections we don't care about.
    void parse_one_section() {
        auto id = _parser->consume_type<uint8_t>();
        if (id != 0) {
            auto it = absl::c_find(_sections_left, id);
            if (it == _sections_left.end()) {
                throw parse_exception(
                  fmt::format("invalid section with id {}", id));
            }
            _sections_left = _sections_left.last(
              std::distance(it++, _sections_left.end()));
        }
        // The size of this section
        auto size = leb128::decode<uint32_t>(_parser);
        enum class section : uint8_t {
            custom = 0x00,
            type = 0x01,
            import = 0x02,
            function = 0x03,
            table = 0x04,
            memory = 0x05,
            global = 0x06,
            exprt = 0x07, // export
            start = 0x08,
            element = 0x09,
            data_count = 0x0C,
            code = 0x0A,
            data = 0x0B,
        };
        switch (static_cast<section>(id)) {
        case section::custom:
            // Skip over custom sections
            _parser->skip(size);
            break;
        case section::type:
            parse_signature_section();
            break;
        case section::import:
            parse_import_section();
            break;
        case section::function:
            parse_function_decl_section();
            break;
        case section::table:
            parse_table_section();
            break;
        case section::memory:
            parse_memories_section();
            break;
        case section::global:
            parse_globals_section();
            break;
        case section::exprt:
            parse_export_section();
            break;
        case section::start:
        case section::element:
        case section::data_count:
        case section::code:
        case section::data:
            // Since we don't need the information from these sections at the
            // moment, we can skip them.
            _parser->skip(size);
            break;
        default:
            throw parse_exception(fmt::format("unknown section id: {}", id));
        }
    }

    void parse_signature_section() {
        auto vector_size = leb128::decode<uint32_t>(_parser);
        if (vector_size > max_functions) {
            throw module_too_large_exception(fmt::format(
              "too large of type section: {}, max: {}",
              vector_size,
              max_functions));
        }
        _func_signatures.reserve(vector_size);
        for (uint32_t i = 0; i < vector_size; ++i) {
            _func_signatures.push_back(parse_signature(_parser));
        }
    }

    void parse_import_section() {
        auto vector_size = leb128::decode<uint32_t>(_parser);
        if (vector_size > max_imports) {
            throw module_too_large_exception(fmt::format(
              "too many imports: {}, max: {}", vector_size, max_imports));
        }

        _imports.reserve(vector_size);
        for (uint32_t i = 0; i < vector_size; ++i) {
            _imports.push_back(parse_one_import());
        }
    }

    module_import parse_one_import() {
        auto module_name = parse_name(_parser);
        auto name = parse_name(_parser);
        auto type = _parser->consume_type<uint8_t>();
        std::optional<import_description> desc;
        switch (type) {
        case 0x00: { // func
            auto funcidx = leb128::decode<uint32_t>(_parser);
            validate_in_range(
              "unknown import function signature", funcidx, _func_signatures);
            auto func = declaration::function{_func_signatures[funcidx]};
            _functions.push_back(func);
            desc = std::move(func);
            break;
        }
        case 0x01: { // table
            auto table = parse_table_type(_parser);
            _tables.push_back(table);
            desc = table;
            break;
        }
        case 0x02: { // memory
            auto mem = parse_memory_type(_parser);
            _memories.push_back(mem);
            desc = mem;
            break;
        }
        case 0x03: { // global
            auto global = parse_global_type(_parser);
            _globals.push_back(global);
            desc = global;
            break;
        }
        default:
            throw parse_exception(fmt::format("unknown import type: {}", type));
        }
        return {
          .module_name = std::move(module_name),
          .item_name = std::move(name),
          .description = desc.value()};
    }

    void parse_function_decl_section() {
        auto vector_size = leb128::decode<uint32_t>(_parser);
        if (vector_size > max_functions) {
            throw module_too_large_exception(fmt::format(
              "too many functions: {}, max: {}", vector_size, max_functions));
        }
        _tables.reserve(max_functions);
        for (uint32_t i = 0; i < vector_size; ++i) {
            auto funcidx = leb128::decode<uint32_t>(_parser);
            validate_in_range(
              "unknown function signature", funcidx, _func_signatures);
            _functions.emplace_back(_func_signatures[funcidx]);
        }
    }

    void parse_table_section() {
        auto vector_size = leb128::decode<uint32_t>(_parser);
        if (vector_size > max_items) {
            throw module_too_large_exception(fmt::format(
              "too many tables: {}, max: {}", vector_size, max_items));
        }
        _tables.reserve(vector_size);
        for (uint32_t i = 0; i < vector_size; ++i) {
            _tables.push_back(parse_table_type(_parser));
        }
    }

    void parse_memories_section() {
        auto vector_size = leb128::decode<uint32_t>(_parser);
        if (vector_size > max_items) {
            throw module_too_large_exception(fmt::format(
              "too many memories: {}, max: {}", vector_size, max_items));
        }
        _memories.reserve(vector_size);
        for (uint32_t i = 0; i < vector_size; ++i) {
            _memories.push_back(parse_memory_type(_parser));
        }
    }

    void parse_globals_section() {
        auto vector_size = leb128::decode<uint32_t>(_parser);
        if (vector_size > max_items) {
            throw module_too_large_exception(fmt::format(
              "too many globals: {}, max: {}", vector_size, max_items));
        }
        _globals.reserve(vector_size);
        for (uint32_t i = 0; i < vector_size; ++i) {
            _globals.push_back(parse_global_type(_parser));
            skip_global_constexpr(_parser);
        }
    }

    void parse_export_section() {
        auto vector_size = leb128::decode<uint32_t>(_parser);
        if (vector_size > max_exports) {
            throw module_too_large_exception(fmt::format(
              "too many exports: {}, max: {}", vector_size, max_exports));
        }
        _exports.reserve(vector_size);
        for (uint32_t i = 0; i < vector_size; ++i) {
            _exports.push_back(parse_one_export());
        }
    }

    module_export parse_one_export() {
        auto name = parse_name(_parser);
        auto type = _parser->consume_type<uint8_t>();
        std::optional<export_description> desc;
        switch (type) {
        case 0x00: { // func
            auto idx = leb128::decode<uint32_t>(_parser);
            validate_in_range("unknown function export", idx, _functions);
            desc = _functions[idx];
            break;
        }
        case 0x01: { // table
            auto idx = leb128::decode<uint32_t>(_parser);
            validate_in_range("unknown function export", idx, _tables);
            desc = _tables[idx];
            break;
        }
        case 0x02: { // memory
            auto idx = leb128::decode<uint32_t>(_parser);
            validate_in_range("unknown memory export", idx, _memories);
            desc = _memories[idx];
            break;
        }
        case 0x03: { // global
            auto idx = leb128::decode<uint32_t>(_parser);
            validate_in_range("unknown global export", idx, _globals);
            desc = _globals[idx];
            break;
        }
        default:
            throw parse_exception(fmt::format("unknown export type: {}", type));
        }
        return {.item_name = std::move(name), .description = desc.value()};
    }

    // In order to properly be able to stream parsing of modules, we need to
    // ensure everything is created in the correct order. The spec enforces
    // that modules are in order to achieve this usecase.
    //
    // NOTE: Custom sections are allowed to be anywhere.
    static constexpr std::array section_order = std::to_array<uint8_t>(
      {1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 10, 11});
    // The ordered list of sections left to encounter.
    //
    // This will become prefix truncated as we visit sections
    std::span<const uint8_t> _sections_left = section_order;

    // The currently parsed data.
    // NOTE these vectors are all explicitly bounded as to prevent large
    // allocations.

    chunked_vector<function_signature> _func_signatures;
    chunked_vector<module_import> _imports;
    chunked_vector<declaration::function> _functions;
    chunked_vector<declaration::table> _tables;
    chunked_vector<declaration::memory> _memories;
    chunked_vector<declaration::global> _globals;
    chunked_vector<module_export> _exports;

    iobuf_parser_base* _parser;
};

} // namespace

ss::future<module_declarations> extract_declarations(iobuf binary_module) {
    iobuf_parser parser(std::move(binary_module));
    module_extractor extractor(&parser);
    try {
        co_return co_await extractor.parse();
    } catch (const std::out_of_range& ex) {
        // Catch a short read in the parser and translate to a parse_exception
        throw parse_exception(ex.what());
    } catch (const leb128::decode_exception& ex) {
        // Catch invalid leb128 and translate to a parse_exception
        throw parse_exception("invalid leb128");
    }
}

} // namespace wasm::parser
