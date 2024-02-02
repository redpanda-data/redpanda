/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "config_store.h"
#include "property.h"

namespace config {

/**
 * Test helper.  A property-like object that does not require
 * a parent config_store.  Useful if you need bindings that can
 * be updated via property changes.
 */
template<typename T>
class mock_property {
public:
    mock_property(T value)
      : _mock_store()
      , _property(
          _mock_store,
          "anonymous",
          "",
          base_property::metadata{.needs_restart = needs_restart::no}) {
        _property.set_value(value);
    }

    void update(T&& value) { _property.update_value(std::move(value)); }

    binding<T> bind() { return _property.bind(); }

    const T& operator()() { return _property(); }

    const T& operator()() const { return _property(); }

    operator T() const { return _property(); } // NOLINT

    template<typename U>
    auto bind(std::function<U(const T&)> conv) -> conversion_binding<U, T> {
        return _property.template bind<U>(std::move(conv));
    }

private:
    config_store _mock_store;
    property<T> _property;
};

} // namespace config
