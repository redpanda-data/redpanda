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

#include "strings/utf8.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(substring_tests) {
    {
        // compiler thinks that ascii_string is unused
        [[maybe_unused]] const std::string_view ascii_string
          = " !\"#$%&'()*+,-./"
            "0123456789:;<=>?@ABCDEFHIJKLMNOPQRSTUVWXYZ[\\]^_`"
            "abcdefghijklmnopqrstuvwxyz{|}~";
        BOOST_ASSERT(
          validate_and_truncate(ascii_string) == ascii_string.size());
    }

    {
        // get a string made of two byte glyphs
        const std::string_view multi_byte_string = "۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩۩";

        // chop the last glyph in half
        [[maybe_unused]] const auto ragged_view = multi_byte_string.substr(
          0, multi_byte_string.size() - 1);

        // make sure that the entire glyph was removed
        BOOST_ASSERT(
          validate_and_truncate(ragged_view) == multi_byte_string.size() - 2);
    }

    {
        // invalid character start sequence
        static constexpr const char invalid_array[2] = {(char)-128, '\0'};
        auto invalid_str = static_cast<const char*>(invalid_array);
        std::string hello{"hello"};
        std::string world{"world"};
        hello.append(invalid_str);
        hello.append(world);
        BOOST_CHECK_THROW(validate_and_truncate(hello), invalid_utf8_exception);
    }
}
