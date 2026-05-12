/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/coordinator/catalog_config.h"

#include "config/configuration.h"

namespace datalake::coordinator {

bool using_glue_catalog() {
    const auto& cfg = config::shard_local_cfg();
    return cfg.iceberg_catalog_type() == config::datalake_catalog_type::rest
           && cfg.iceberg_rest_catalog_authentication_mode()
                == config::datalake_catalog_auth_mode::aws_sigv4
           && cfg.iceberg_rest_catalog_aws_service_name() == "glue";
}

iceberg::field_name_comparison resolve_field_name_comparison() {
    const auto effective
      = config::shard_local_cfg().iceberg_schema_case_insensitive();
    if (effective == model::iceberg_schema_case_insensitive::yes) {
        return iceberg::field_name_comparison::lower_case;
    }
    if (
      effective == model::iceberg_schema_case_insensitive::auto_
      && using_glue_catalog()) {
        return iceberg::field_name_comparison::lower_case;
    }
    return iceberg::field_name_comparison::verbatim;
}

} // namespace datalake::coordinator
