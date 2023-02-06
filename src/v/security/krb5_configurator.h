/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "config/property.h"

#include <seastar/core/sstring.hh>

namespace security::krb5 {

class configurator {
public:
    explicit configurator(config::binding<ss::sstring>&& krb5_config);

private:
    std::function<void()> krb5_config_setter();
    config::binding<ss::sstring> _krb5_config;
};

} // namespace security::krb5
