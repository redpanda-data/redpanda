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

#include "utils/utf8.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(control_chars_default_thrower) {
    {
        const std::string_view good_string
          = " !\"#$%&'()*+,-./"
            "0123456789:;<=>?@ABCDEFHIJKLMNOPQRSTUVWXYZ[\\]^_`"
            "abcdefghijklmnopqrstuvwxyz{|}~";
        BOOST_CHECK_NO_THROW(validate_no_control(good_string));
    }
    for (char ch = 1; ch <= 0x1f; ch++) {
        const std::string_view temp{&ch, 1};
        BOOST_CHECK_THROW(validate_no_control(temp), std::runtime_error);
    }

    {
        const char ch = '\x7f';
        const std::string_view temp{&ch, 1};
        BOOST_CHECK_THROW(validate_no_control(temp), std::runtime_error);
    }

    {
        const std::string_view bad_string
          = "abcdefghijklmnopqrstuvwxyz\nABCDEFGHIJKLMNOPQRSTUVWXYZ";

        BOOST_CHECK_THROW(validate_no_control(bad_string), std::runtime_error);
    }
}

struct my_check_exception {
    [[noreturn]] [[gnu::cold]] void conversion_error() {
        throw std::invalid_argument("invalid argument");
    }
};

BOOST_AUTO_TEST_CASE(control_chars_customized_thrower) {
    const std::string_view bad_string
      = "abcdefghijklmnopqrstuvwxyz\nABCDEFGHIJKLMNOPQRSTUVWXYZ";

    BOOST_CHECK_THROW(
      validate_no_control(bad_string, my_check_exception{}),
      std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(control_char_replace) {
    const std::string_view test_string = "hello,\x7f there";
    BOOST_CHECK_EQUAL(
      "hello,\xe2\x90\xa1 there", replace_control_chars_in_string(test_string));

    const std::string_view clear_string = "hello, there";

    BOOST_CHECK_EQUAL(
      clear_string, replace_control_chars_in_string(clear_string));

    const std::string_view test_string_newline = "hello,\nthere\r";

    BOOST_CHECK_EQUAL(
      "hello,\xe2\x90\x8athere\xe2\x90\x8d",
      replace_control_chars_in_string(test_string_newline));
}
