# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this s
from rptest.services.redpanda import CloudStorageType, get_cloud_storage_type


def supported_storage_types():
    return get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
