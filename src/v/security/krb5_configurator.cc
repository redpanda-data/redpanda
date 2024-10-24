/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/krb5_configurator.h"

#include "config/property.h"

namespace security::krb5 {

configurator::configurator(config::binding<ss::sstring>&& krb5_config)
  : _krb5_config(std::move(krb5_config)) {
    if (ss::this_shard_id() == ss::shard_id{0}) {
        krb5_config_setter();
        _krb5_config.watch(krb5_config_setter());
    }
}

std::function<void()> configurator::krb5_config_setter() {
    return [this]() { setenv("KRB5_CONFIG", _krb5_config().c_str(), 1); };
}

} // namespace security::krb5
