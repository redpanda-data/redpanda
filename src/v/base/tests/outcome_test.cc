// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE outcome
#include "base/outcome.h"

#include <boost/test/unit_test.hpp>

#include <string>

// This is the custom error code enum
enum class conversion_errc {
    success = 0, // 0 should _never_ represent an error
    empty_string = 1,
    illegal_char = 2,
    too_long = 3,
};

namespace std {
// Tell the C++ 11 STL metaprogramming that enum conversion_errc
// is registered with the standard error code system
template<>
struct is_error_code_enum<conversion_errc> : true_type {};
} // namespace std

namespace detail {
// Define a custom error code category derived from std::error_category
class conversion_errc_category : public std::error_category {
public:
    // Return a short descriptive name for the category
    virtual const char* name() const noexcept override final {
        return "ConversionError";
    }
    // Return what each enum means in text
    virtual std::string message(int c) const override final {
        switch (static_cast<conversion_errc>(c)) {
        case conversion_errc::success:
            return "conversion successful";
        case conversion_errc::empty_string:
            return "converting empty string";
        case conversion_errc::illegal_char:
            return "got non-digit char when converting to a number";
        case conversion_errc::too_long:
            return "the number would not fit into memory";
        default:
            return "unknown";
        }
    }
    // OPTIONAL: Allow generic error conditions to be compared to me
    virtual std::error_condition
    default_error_condition(int c) const noexcept override final {
        switch (static_cast<conversion_errc>(c)) {
        case conversion_errc::empty_string:
            return make_error_condition(std::errc::invalid_argument);
        case conversion_errc::illegal_char:
            return make_error_condition(std::errc::invalid_argument);
        case conversion_errc::too_long:
            return make_error_condition(std::errc::result_out_of_range);
        default:
            // I have no mapping for this code
            return std::error_condition(c, *this);
        }
    }
};
} // namespace detail

// Declare a global function returning a static instance of the custom category
extern inline detail::conversion_errc_category& conversion_errc_category() {
    static detail::conversion_errc_category c;
    return c;
}

// Overload the global make_error_code() free function with our
// custom enum. It will be found via ADL by the compiler if needed.
inline std::error_code make_error_code(conversion_errc e) {
    return {static_cast<int>(e), conversion_errc_category()};
}

// Main test function as per the documentation
inline result<int> convert(const std::string& str) noexcept {
    if (str.empty()) return conversion_errc::empty_string;

    if (!std::all_of(str.begin(), str.end(), ::isdigit))
        return conversion_errc::illegal_char;

    if (str.length() > 9) return conversion_errc::too_long;

    return atoi(str.c_str());
}

BOOST_AUTO_TEST_CASE(test_implicit_conversions_to_from_error_code) {
    // This is the main example and stresses our re-definitions
    // from outcome.h
    // Note that we can now supply conversion_errc directly to error_code
    std::error_code ec = conversion_errc::illegal_char;

    std::cout
      << "conversion_errc::illegal_char is printed by std::error_code as " << ec
      << " with explanatory message " << ec.message() << std::endl;

    // We can compare conversion_errc containing error codes to generic
    // conditions
    std::cout << "ec is equivalent to std::errc::invalid_argument = "
              << (ec == std::errc::invalid_argument) << std::endl;
    std::cout << "ec is equivalent to std::errc::result_out_of_range = "
              << (ec == std::errc::result_out_of_range) << std::endl;

    BOOST_REQUIRE(ec == std::errc::invalid_argument);
    BOOST_REQUIRE(ec != std::errc::result_out_of_range);
}

inline result<int> test_propagate(const std::string& input) {
    auto i = convert(input);
    if (!i) {
        return i;
    }
    return i.value() + 1;
}

BOOST_AUTO_TEST_CASE(test_propagate_redefinition) {
    {
        result<int> r = convert("1234");
        BOOST_REQUIRE(r);
    }
    {
        result<int> r = convert("123412341234123412341234123412341234");
        BOOST_REQUIRE(r.error() == conversion_errc::too_long);
    }
    {
        auto i = test_propagate("12341234123412341234123412341");
        BOOST_REQUIRE(i.error() == conversion_errc::too_long);
    }

    /// test result<void>
    result<void> i = outcome::success();
    BOOST_REQUIRE(i);
}
