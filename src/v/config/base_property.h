/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "config/validation_error.h"
#include "json/json.h"
#include "seastarx.h"

#include <seastar/util/bool_class.hh>

#include <yaml-cpp/yaml.h>

#include <any>
#include <iosfwd>
#include <string>

namespace config {

class config_store;
using required = ss::bool_class<struct required_tag>;

class base_property {
public:
    base_property(
      config_store& conf,
      std::string_view name,
      std::string_view desc,
      required req);

    const std::string_view& name() const { return _name; }
    const std::string_view& desc() const { return _desc; }

    const required is_required() const { return _required; }

    // this serializes the property value. a full configuration serialization is
    // performed in config_store::to_json where the json object key is taken
    // from the property name.
    virtual void
    to_json(rapidjson::Writer<rapidjson::StringBuffer>& w) const = 0;

    virtual void print(std::ostream&) const = 0;
    virtual void set_value(YAML::Node) = 0;
    virtual void set_value(std::any) = 0;
    virtual void reset() = 0;
    virtual std::optional<validation_error> validate() const = 0;
    virtual base_property& operator=(const base_property&) = 0;
    virtual ~base_property() noexcept = default;

private:
    friend std::ostream& operator<<(std::ostream&, const base_property&);
    std::string_view _name;
    std::string_view _desc;
    required _required;
};
}; // namespace config
