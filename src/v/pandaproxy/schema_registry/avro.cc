/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/avro.h"

#include "pandaproxy/schema_registry/error.h"

#include <avro/Compiler.hh>
#include <avro/Exception.hh>
#include <avro/GenericDatum.hh>
#include <avro/ValidSchema.hh>

namespace pandaproxy::schema_registry {

namespace {

bool check_compatible(avro::Node& reader, avro::Node& writer) {
    if (reader.type() == writer.type()) {
        // Do a quick check first
        if (!writer.resolve(reader)) {
            return false;
        }
        if (reader.type() == avro::Type::AVRO_RECORD) {
            // Recursively check fields
            for (size_t r_idx = 0; r_idx < reader.names(); ++r_idx) {
                size_t w_idx{0};
                if (writer.nameIndex(reader.nameAt(int(r_idx)), w_idx)) {
                    // schemas for fields with the same name in both records are
                    // resolved recursively.
                    if (!check_compatible(
                          *reader.leafAt(int(r_idx)),
                          *writer.leafAt(int(w_idx)))) {
                        return false;
                    }
                } else if (
                  reader.defaultValueAt(int(r_idx)).type() == avro::AVRO_NULL) {
                    // if the reader's record schema has a field with no default
                    // value, and writer's schema does not have a field with the
                    // same name, an error is signalled.
                    return false;
                }
            }
            return true;
        } else if (reader.type() == avro::AVRO_ENUM) {
            // if the writer's symbol is not present in the reader's enum and
            // the reader has a default value, then that value is used,
            // otherwise an error is signalled.
            for (size_t w_idx = 0; w_idx < writer.names(); ++w_idx) {
                size_t r_idx{0};
                if (!reader.nameIndex(writer.nameAt(int(w_idx)), r_idx)) {
                    // TODO(Ben): NodeEnum doesn't support defaults.
                    return false;
                }
            }
        } else if (reader.type() == avro::AVRO_UNION) {
            // The first schema in the reader's union that matches the selected
            // writer's union schema is recursively resolved against it. if none
            // match, an error is signalled.
            //
            // Alternatively, any reader must match every writer schema
            for (size_t w_idx = 0; w_idx < writer.leaves(); ++w_idx) {
                bool is_compat = false;
                for (size_t r_idx = 0; r_idx < reader.leaves(); ++r_idx) {
                    if (check_compatible(
                          *reader.leafAt(int(r_idx)),
                          *writer.leafAt(int(w_idx)))) {
                        is_compat = true;
                    }
                }
                if (!is_compat) {
                    return false;
                }
            }
            return true;
        }
    } else if (reader.type() == avro::AVRO_UNION) {
        // The first schema in the reader's union that matches the writer's
        // schema is recursively resolved against it. If none match, an error is
        // signalled.
        //
        // Alternatively, any schema in the reader union must match writer.
        for (size_t r_idx = 0; r_idx < reader.leaves(); ++r_idx) {
            if (check_compatible(*reader.leafAt(int(r_idx)), writer)) {
                return true;
            }
        }
        return false;
    } else if (writer.type() == avro::AVRO_UNION) {
        // If the reader's schema matches the selected writer's schema, it is
        // recursively resolved against it. If they do not match, an error is
        // signalled.
        //
        // Alternatively, reader must match all schema in writer union.
        for (size_t w_idx = 0; w_idx < writer.leaves(); ++w_idx) {
            if (!check_compatible(reader, *writer.leafAt(int(w_idx)))) {
                return false;
            }
        }
        return true;
    }
    return writer.resolve(reader) == avro::RESOLVE_MATCH;
}

} // namespace

result<avro_schema_definition>
make_avro_schema_definition(std::string_view sv) {
    try {
        return avro_schema_definition{avro::compileJsonSchemaFromMemory(
          reinterpret_cast<const uint8_t*>(sv.data()), sv.length())};
    } catch (const avro::Exception& e) {
        return error_info{
          error_code::schema_invalid,
          fmt::format("Invalid schema {}", e.what())};
    }
}

bool check_compatible(
  const avro_schema_definition& reader, const avro_schema_definition& writer) {
    return check_compatible(*reader().root(), *writer().root());
}

} // namespace pandaproxy::schema_registry
