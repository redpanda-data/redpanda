/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/data_migration_types.h"
#include "json/document.h"
#include "model/metadata.h"

model::topic_namespace parse_topic_namespace(json::Value& json);

cluster::data_migrations::inbound_topic parse_inbound_topic(json::Value& json);

chunked_vector<cluster::data_migrations::inbound_topic>
parse_inbound_topics(json::Value& json);
chunked_vector<model::topic_namespace> parse_topics(json::Value& json);
