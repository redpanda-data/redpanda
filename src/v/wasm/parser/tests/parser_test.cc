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

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "container/fragmented_vector.h"
#include "gtest/gtest.h"
#include "wasm/parser/parser.h"

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>
#include <utility>
#include <wasm.h>
#include <wasmtime.h>

namespace wasm::parser {

namespace {
bytes wat2wasm(std::string_view wat) {
    wasm_byte_vec_t wasm_bytes;
    wasm_byte_vec_new_empty(&wasm_bytes);
    wasmtime_error_t* error = wasmtime_wat2wasm(
      wat.data(), wat.size(), &wasm_bytes);
    bytes b(reinterpret_cast<uint8_t*>(wasm_bytes.data), wasm_bytes.size);
    wasm_byte_vec_delete(&wasm_bytes);
    if (error != nullptr) {
        throw std::runtime_error("invalid wat");
        wasmtime_error_delete(error);
    }
    return b;
}

struct test_data {
    std::string wat;
    std::vector<module_export> exports;
    std::vector<module_import> imports;
};

void PrintTo(const test_data& d, std::ostream* os) {
    constexpr size_t max_len = 128;
    *os << d.wat.substr(0, std::min(d.wat.size(), max_len));
}

class ParserTest : public testing::TestWithParam<test_data> {};

TEST_P(ParserTest, ExtractDeclarations) {
    const auto& testcase = GetParam();
    bytes wasm = wat2wasm(testcase.wat);
    auto decls = extract_declarations(bytes_to_iobuf(wasm)).get();
    module_declarations expected = {
      .exports = chunked_vector<module_export>(
        testcase.exports.begin(), testcase.exports.end()),
      .imports = chunked_vector<module_import>(
        testcase.imports.begin(), testcase.imports.end()),
    };
    EXPECT_EQ(decls, std::ref(expected));
}

module_export func(
  ss::sstring name,
  std::vector<val_type> params,
  std::vector<val_type> results) {
    return {
      .item_name = std::move(name),
      .description = declaration::function{{params, results}},
    };
}

module_import func(
  ss::sstring mod,
  ss::sstring name,
  std::vector<val_type> params,
  std::vector<val_type> results) {
    return {
      .module_name = std::move(mod),
      .item_name = std::move(name),
      .description = declaration::function{{params, results}},
    };
}

module_export global(ss::sstring name, val_type ty) {
    return {
      .item_name = std::move(name),
      .description = declaration::global{ty, false},
    };
}

module_import global(ss::sstring mod, ss::sstring name, val_type ty) {
    return {
      .module_name = std::move(mod),
      .item_name = std::move(name),
      .description = declaration::global{ty, false},
    };
}

module_export table(ss::sstring name, val_type ty, declaration::limits limits) {
    return {
      .item_name = std::move(name),
      .description = declaration::table{ty, limits},
    };
}

module_import table(
  ss::sstring mod, ss::sstring name, val_type ty, declaration::limits limits) {
    return {
      .module_name = std::move(mod),
      .item_name = std::move(name),
      .description = declaration::table{ty, limits},
    };
}

module_export memory(ss::sstring name, declaration::limits limits) {
    return {
      .item_name = std::move(name),
      .description = declaration::memory{limits},
    };
}

module_import
memory(ss::sstring mod, ss::sstring name, declaration::limits limits) {
    return {
      .module_name = std::move(mod),
      .item_name = std::move(name),
      .description = declaration::memory{limits},
    };
}

// Many of these tests are from the offical Wasm test suite.
INSTANTIATE_TEST_SUITE_P(
  WatTests,
  ParserTest,
  testing::ValuesIn<std::vector<test_data>>(
    {
      {
        .wat = R"WAT((module))WAT",
        .exports = {},
        .imports = {},
      },
      {
        .wat = R"WAT(
(module
  (import "console" "log" (func $log (param i32)))
  (global $from_js (import "env" "from_js") i32)
  (global $from_wasm i32 (i32.const 10))
  (func $main
    global.get $from_js
    global.get $from_wasm
    i32.add
    call $log
  )
  (export "_start" (func $main))
)
)WAT",
        .exports = {
          func("_start", {}, {})
        },
        .imports = {
          func("console", "log", {val_type::i32}, {}),
          global("env", "from_js", val_type::i32),
        },
      },
      {
        .wat = R"WAT(
(module
  (import "spectest" "global_i32" (global i32))
  (global (import "spectest" "global_i32") i32)

  (import "spectest" "global_i32" (global $x i32))
  (global $y (import "spectest" "global_i32") i32)

  (import "spectest" "global_i64" (global i64))
  (import "spectest" "global_f32" (global f32))
  (import "spectest" "global_f64" (global f64))

  (func (export "get-0") (result i32) (global.get 0))
  (func (export "get-1") (result i32) (global.get 1))
  (func (export "get-x") (result i32) (global.get $x))
  (func (export "get-y") (result i32) (global.get $y))
  (func (export "get-4") (result i64) (global.get 4))
  (func (export "get-5") (result f32) (global.get 5))
  (func (export "get-6") (result f64) (global.get 6))
)
)WAT",
        .exports = {
          func("get-0", {}, {val_type::i32}),
          func("get-1", {}, {val_type::i32}),
          func("get-x", {}, {val_type::i32}),
          func("get-y", {}, {val_type::i32}),
          func("get-4", {}, {val_type::i64}),
          func("get-5", {}, {val_type::f32}),
          func("get-6", {}, {val_type::f64}),
        },
        .imports = {
          global("spectest", "global_i32", val_type::i32),
          global("spectest", "global_i32", val_type::i32),
          global("spectest", "global_i32", val_type::i32),
          global("spectest", "global_i32", val_type::i32),
          global("spectest", "global_i64", val_type::i64),
          global("spectest", "global_f32", val_type::f32),
          global("spectest", "global_f64", val_type::f64),
        },
      },
      {
        .wat = R"WAT(
(module
  (type (func (result i32)))
  (import "spectest" "table" (table $tab 10 20 funcref))
  (elem (table $tab) (i32.const 1) func $f $g)

  (func (export "call") (param i32) (result i32)
    (call_indirect $tab (type 0) (local.get 0))
  )
  (func $f (result i32) (i32.const 11))
  (func $g (result i32) (i32.const 22))
)
)WAT",
        .exports = {func("call", {val_type::i32}, {val_type::i32})},
        .imports = {table("spectest", "table", val_type::funcref, {.min = 10, .max = 20})},
      },
      {
        .wat = R"WAT(
(module
  (import "spectest" "memory" (memory 1 2))
  (data (memory 0) (i32.const 10) "\10")

  (func (export "load") (param i32) (result i32) (i32.load (local.get 0)))
)
)WAT",
        .exports = {func("load", {val_type::i32}, {val_type::i32})},
        .imports = {memory("spectest", "memory", {.min = 1, .max = 2})},
      },
      {
        .wat = R"WAT(
(module (memory (export "a") 0))
)WAT",
        .exports = {memory("a", {})},
        .imports = {},
      },
      {
        .wat = R"WAT(
(module (table 0 funcref) (export "a" (table 0)))
)WAT",
        .exports = {table("a", val_type::funcref, {})},
        .imports = {},
      },
      {
        .wat = R"WAT(
(module (global i32 (i32.const 0)) (export "a" (global 0)))
)WAT",
        .exports = {global("a", val_type::i32)},
        .imports = {},
      },
      {
        .wat = R"WAT(
(module (func) (export "a" (func 0)))
)WAT",
        .exports = {func("a", {}, {})},
        .imports = {},
      },
      {
        .wat = R"WAT(
(module
  (type $func_i32 (func (param i32)))
  (type $func_i64 (func (param i64)))
  (type $func_f32 (func (param f32)))
  (type $func_f64 (func (param f64)))

  (import "spectest" "print_i32" (func (param i32)))
  (func (import "spectest" "print_i64") (param i64))
  (import "spectest" "print_i32" (func $print_i32 (param i32)))
  (import "spectest" "print_i64" (func $print_i64 (param i64)))
  (import "spectest" "print_f32" (func $print_f32 (param f32)))
  (import "spectest" "print_f64" (func $print_f64 (param f64)))
  (import "spectest" "print_i32_f32" (func $print_i32_f32 (param i32 f32)))
  (import "spectest" "print_f64_f64" (func $print_f64_f64 (param f64 f64)))
  (func $print_i32-2 (import "spectest" "print_i32") (param i32))
  (func $print_f64-2 (import "spectest" "print_f64") (param f64))
  (import "test" "func-i64->i64" (func $i64->i64 (param i64) (result i64)))

  (func (export "p1") (import "spectest" "print_i32") (param i32))
  (func $p (export "p2") (import "spectest" "print_i32") (param i32))
  (func (export "p3") (export "p4") (import "spectest" "print_i32") (param i32))
  (func (export "p5") (import "spectest" "print_i32") (type 0))
  (func (export "p6") (import "spectest" "print_i32") (type 0) (param i32) (result))

  (import "spectest" "print_i32" (func (type $forward)))
  (func (import "spectest" "print_i32") (type $forward))
  (type $forward (func (param i32)))

  (table funcref (elem $print_i32 $print_f64))

  (func (export "print32") (param $i i32)
    (local $x f32)
    (local.set $x (f32.convert_i32_s (local.get $i)))
    (call 0 (local.get $i))
    (call $print_i32_f32
      (i32.add (local.get $i) (i32.const 1))
      (f32.const 42)
    )
    (call $print_i32 (local.get $i))
    (call $print_i32-2 (local.get $i))
    (call $print_f32 (local.get $x))
    (call_indirect (type $func_i32) (local.get $i) (i32.const 0))
  )

  (func (export "print64") (param $i i64)
    (local $x f64)
    (local.set $x (f64.convert_i64_s (call $i64->i64 (local.get $i))))
    (call 1 (local.get $i))
    (call $print_f64_f64
      (f64.add (local.get $x) (f64.const 1))
      (f64.const 53)
    )
    (call $print_i64 (local.get $i))
    (call $print_f64 (local.get $x))
    (call $print_f64-2 (local.get $x))
    (call_indirect (type $func_f64) (local.get $x) (i32.const 1))
  )
)
)WAT",
        .exports = {
          func("p1", {val_type::i32}, {}),
          func("p2", {val_type::i32}, {}),
          func("p3", {val_type::i32}, {}),
          func("p4", {val_type::i32}, {}),
          func("p5", {val_type::i32}, {}),
          func("p6", {val_type::i32}, {}),
          func("print32", {val_type::i32}, {}),
          func("print64", {val_type::i64}, {}),
        },
        .imports = {
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i64", {val_type::i64}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i64", {val_type::i64}, {}),
          func("spectest", "print_f32", {val_type::f32}, {}),
          func("spectest", "print_f64", {val_type::f64}, {}),
          func("spectest", "print_i32_f32", {val_type::i32, val_type::f32}, {}),
          func("spectest", "print_f64_f64", {val_type::f64, val_type::f64}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_f64", {val_type::f64}, {}),
          func("test", "func-i64->i64", {val_type::i64}, {val_type::i64}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
          func("spectest", "print_i32", {val_type::i32}, {}),
        },
      },
      {
        .wat = R"WAT(
(module
  (memory 1)
  (data (i32.const 0) "a")
)
)WAT",
        .exports = {},
        .imports = {},
      },
      {
        .wat = R"WAT(
(module
  (global (;0;) (mut i32) (i32.const 306464))
  (global (;1;) i32 (i32.const 0))
  (global (;2;) i32 (i32.const 239492))
  (global (;3;) i32 (i32.const 448))
  (global (;4;) i32 (i32.const 1))
  (global (;5;) i32 (i32.const 193520))
  (global (;6;) i32 (i32.const 449))
  (global (;7;) i32 (i32.const 234008))
  (global (;8;) i32 (i32.const 233968))
  (global (;9;) i32 (i32.const 233868))

  (global $a i32 (i32.const -2))
  (global (;3;) f32 (f32.const -3))
  (global (;4;) f64 (f64.const -4))
  (global $b i64 (i64.const -5))

  (global $x (mut i32) (i32.const -12))
  (global (;7;) (mut f32) (f32.const -13))
  (global (;8;) (mut f64) (f64.const -14))
  (global $y (mut i64) (i64.const -15))

  (global $z1 i32 (global.get 0))
  (global $z2 i64 (global.get 1))

  (global $r externref (ref.null extern))
  (global $mr (mut externref) (ref.null extern))
  (global funcref (ref.null func))
)
)WAT",
        .exports = {},
        .imports = {},
      },
}));

} // namespace

} // namespace wasm::parser
