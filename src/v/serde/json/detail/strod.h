/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 *
 * This file includes code from RapidJSON (https://rapidjson.org/)
 *
 * Copyright (C) 2015 THL A29 Limited, a Tencent company, and Milo Yip.
 *
 * Licensed under the MIT License (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License
 * at http://opensource.org/licenses/MIT
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#pragma once

#include "serde/json/detail/pow10.h"

namespace serde::json::detail {

inline double strod_fast_path(double significand, int exp) {
    if (exp < -308) {
        return 0.0;
    } else if (exp >= 0) {
        return significand * pow10(exp);
    } else {
        return significand / pow10(-exp);
    }
}

inline double strtod_normal_precision(double d, int p) {
    if (p < -308) {
        // Prevent expSum < -308, making Pow10(p) = 0
        d = strod_fast_path(d, -308);
        d = strod_fast_path(d, p + 308);
    } else {
        d = strod_fast_path(d, p);
    }
    return d;
}
}; // namespace serde::json::detail
