/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/json/tests/dom_serde.h"
#include "test_utils/runfiles.h"
#include "test_utils/test.h"
#include "utils/file_io.h"

#include <gtest/gtest.h>

using namespace serde::json;

constexpr auto golden = R"(json_value(array(
  string(JSON Test Pattern pass1)
  object(
    key(object with 1 member) :
      array(
        string(array with 1 element)
      )
  )
  object()
  array()
  int(-42)
  true
  false
  null
  object(
    key(integer) :
      int(1234567890)
    key(real) :
      double(-9876.54321)
    key(e) :
      double(1.23456789e-13)
    key(E) :
      double(1.2345678900000002e+34)
    key() :
      double(2.3456789011999997e+76)
    key(zero) :
      int(0)
    key(one) :
      int(1)
    key(space) :
      string( )
    key(quote) :
      string(\")
    key(backslash) :
      string(\\)
    key(controls) :
      string(\x08\x0c\n\r\t)
    key(slash) :
      string(/ & /)
    key(alpha) :
      string(abcdefghijklmnopqrstuvwyz)
    key(ALPHA) :
      string(ABCDEFGHIJKLMNOPQRSTUVWYZ)
    key(digit) :
      string(0123456789)
    key(0123456789) :
      string(digit)
    key(special) :
      string(`1~!@#$%^&*()_+-={\':[,]}|;.</>?)
    key(hex) :
      string(\xc4\xa3\xe4\x95\xa7\xe8\xa6\xab\xec\xb7\xaf\xea\xaf\x8d\xee\xbd\x8a)
    key(true) :
      true
    key(false) :
      false
    key(null) :
      null
    key(array) :
      array()
    key(object) :
      object()
    key(address) :
      string(50 St. James Street)
    key(url) :
      string(http://www.JSON.org/)
    key(comment) :
      string(// /* <!-- --)
    key(# -- --> */) :
      string( )
    key( s p a c e d ) :
      array(
        int(1)
        int(2)
        int(3)
        int(4)
        int(5)
        int(6)
        int(7)
      )
    key(compact) :
      array(
        int(1)
        int(2)
        int(3)
        int(4)
        int(5)
        int(6)
        int(7)
      )
    key(jsontext) :
      string({\"object with 1 member\":[\"array with 1 element\"]})
    key(quotes) :
      string(&#34; \" %22 0x22 034 &#x22;)
    key(/\\\"\xec\xab\xbe\xeb\xaa\xbe\xea\xae\x98\xef\xb3\x9e\xeb\xb3\x9a\xee\xbd\x8a\x08\x0c\n\r\t`1~!@#$%^&*()_+-=[]{}|;:\',./<>?) :
      string(A key can be any string)
  )
  double(0.5)
  double(98.6)
  double(99.44)
  int(1066)
  double(10)
  double(1)
  double(0.1)
  double(1)
  double(2)
  double(2)
  string(rosebud)
)))";

TEST_CORO(dom_serde_test, json_checker_pass1) {
    auto test_case_path = test_utils::get_runfile_path(
      "src/v/serde/json/tests/testdata/jsonchecker/pass1.json");

    auto contents = co_await read_fully(test_case_path);

    auto v = co_await test::dom::parse_document_serde(std::move(contents));
    std::stringstream ss;
    ss << v;

    // Uncomment when you need to update golden.
    // std::cout << "Parsed JSON:\n" << ss.str() << std::endl;

    ASSERT_EQ_CORO(golden, ss.str());
}
