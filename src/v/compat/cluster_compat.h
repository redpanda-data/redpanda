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

#include "cluster/types.h"
#include "compat/check.h"
#include "compat/cluster_generator.h"
#include "compat/cluster_json.h"
#include "compat/json.h"

#define GEN_COMPAT_CHECK(Type, ToJson, FromJson)                               \
    template<>                                                                 \
    struct compat_check<Type> {                                                \
        static constexpr std::string_view name = #Type;                        \
                                                                               \
        static std::vector<Type> create_test_cases() {                         \
            return generate_instances<Type>();                                 \
        }                                                                      \
                                                                               \
        static void to_json(Type obj, json::Writer<json::StringBuffer>& wr) {  \
            ToJson;                                                            \
        }                                                                      \
                                                                               \
        static Type from_json(json::Value& rd) {                               \
            Type obj;                                                          \
            FromJson;                                                          \
            return obj;                                                        \
        }                                                                      \
                                                                               \
        static std::vector<compat_binary> to_binary(Type obj) {                \
            return compat_binary::serde_and_adl(obj);                          \
        }                                                                      \
                                                                               \
        static bool check(Type obj, compat_binary test) {                      \
            return verify_adl_or_serde(obj, std::move(test));                  \
        }                                                                      \
    };

namespace compat {

GEN_COMPAT_CHECK(
  cluster::config_status,
  {
      json_write(node);
      json_write(version);
      json_write(restart);
      json_write(unknown);
      json_write(invalid);
  },
  {
      json_read(node);
      json_read(version);
      json_read(restart);
      json_read(unknown);
      json_read(invalid);
  });

} // namespace compat
