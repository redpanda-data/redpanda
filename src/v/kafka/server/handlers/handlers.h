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
#pragma once

#include "kafka/server/handlers/alter_configs.h"
#include "kafka/server/handlers/api_versions.h"
#include "kafka/server/handlers/create_acls.h"
#include "kafka/server/handlers/create_topics.h"
#include "kafka/server/handlers/delete_acls.h"
#include "kafka/server/handlers/delete_groups.h"
#include "kafka/server/handlers/delete_topics.h"
#include "kafka/server/handlers/describe_acls.h"
#include "kafka/server/handlers/describe_configs.h"
#include "kafka/server/handlers/describe_groups.h"
#include "kafka/server/handlers/describe_log_dirs.h"
#include "kafka/server/handlers/fetch.h"
#include "kafka/server/handlers/find_coordinator.h"
#include "kafka/server/handlers/heartbeat.h"
#include "kafka/server/handlers/incremental_alter_configs.h"
#include "kafka/server/handlers/init_producer_id.h"
#include "kafka/server/handlers/join_group.h"
#include "kafka/server/handlers/leave_group.h"
#include "kafka/server/handlers/list_groups.h"
#include "kafka/server/handlers/list_offsets.h"
#include "kafka/server/handlers/metadata.h"
#include "kafka/server/handlers/offset_commit.h"
#include "kafka/server/handlers/offset_fetch.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/server/handlers/sasl_authenticate.h"
#include "kafka/server/handlers/sasl_handshake.h"
#include "kafka/server/handlers/sync_group.h"
#include "kafka/server/handlers/txn_offset_commit.h"
