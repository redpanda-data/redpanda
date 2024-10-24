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

#include "strings/utf8.h"

thread_local bool permit_unsafe_log_operation::_flag = false;

bool is_valid_utf8(std::string_view s) {
    bool is_valid = true;
    class default_utf8_checker {
    public:
        explicit default_utf8_checker(bool* is_valid)
          : _is_valid(is_valid) {}

        void conversion_error() { *_is_valid = false; }

    private:
        bool* _is_valid;
    };
    validate_utf8(s, default_utf8_checker{&is_valid});
    return is_valid;
}
