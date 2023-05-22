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

#include "json/document.h"
#include "model/fundamental.h"
#include "model/record_batch_types.h"

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string_view>

namespace storage {

const std::string failure_injector_schema = R"(
{
    "type": "object",
    "properties": {
        "seed": {
            "type": "integer"
        },
        "ntps": {
            "type": "array",
            "items": { "$ref": "#/definitions/ntp_failure_config" }
        }
    },
    "additionalProperties": false,
    "required": ["seed", "ntps"],
    "definitions": {
        "ntp_failure_config": {
            "type": "object",
            "properties": {
                "namespace": {
                    "type": "string"
                },
                "topic": {
                    "type": "string"
                },
                "partition": {
                    "type": "integer",
                    "minimum": 0
                },
                "failure_configs": {
                    "type": "object",
                    "properties": {
                        "write": { "$ref": "#/definitions/failure_config" },
                        "falloc": { "$ref": "#/definitions/failure_config" },
                        "flush": { "$ref": "#/definitions/failure_config" },
                        "truncate": { "$ref": "#/definitions/failure_config" },
                        "close": { "$ref": "#/definitions/failure_config" }
                    }
                }
            },
            "additionalProperties": false,
            "required": ["namespace", "topic", "partition", "failure_configs"]
        },
        "failure_config": {
            "type": "object",
            "properties": {
                "batch_type": {
                    "type": "string"
                },
                "failure_probability": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100
                },
                "delay_probability": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100
                },
                "min_delay_ms": {
                    "type": "integer",
                    "minimum": 0
                },
                "max_delay_ms": {
                    "type": "integer",
                    "minimum": 0
                }
            },
            "additionalProperties": false
        }
    }
}
)";

enum class failable_op_type : uint8_t { write, falloc, truncate, flush, close };
std::ostream& operator<<(std::ostream& o, failable_op_type op);
std::istream& operator>>(std::istream& o, failable_op_type op);

struct failable_op_config {
    failable_op_type op_type;
    std::optional<model::record_batch_type> batch_type;
    std::optional<double> failure_probability;
    std::optional<double> delay_probability;
    std::optional<int> min_delay_ms;
    std::optional<int> max_delay_ms;
};

struct ntp_failure_injection_config {
    model::ntp ntp;
    size_t seed;
    std::vector<failable_op_config> op_configs;
};

// Copy held by the log injector file
struct ntp_sanitizer_config {
    bool sanitize_only;
    std::optional<ntp_failure_injection_config> finjection_cfg;
};

std::ostream& operator<<(std::ostream& o, const ntp_sanitizer_config& cfg);

// Held by log manager
class file_sanitize_config {
public:
    explicit file_sanitize_config(bool sanitize_only = false);
    explicit file_sanitize_config(json::Document);

    std::optional<ntp_sanitizer_config>
    get_config_for_ntp(const model::ntp&) const;

    friend std::ostream&
    operator<<(std::ostream& o, const file_sanitize_config& cfg);

private:
    bool _sanitize_only{false};
    absl::flat_hash_map<model::ntp, ntp_sanitizer_config> _ntp_failure_configs;
};

ss::future<std::optional<file_sanitize_config>>
make_finjector_file_config(std::filesystem::path config_path);

file_sanitize_config make_sanitized_file_config();
} // namespace storage
