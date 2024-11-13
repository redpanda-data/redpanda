# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this s
from rptest.services.redpanda import CloudStorageType, get_cloud_provider, get_cloud_storage_type
from itertools import product

# gcp also uses S3 as storage type
# Currently we only run tests in ec2 + S3 combination
# as the query engine deployment was tested only in ec2.
# The credential plumbing (IAM roles, credential providers) probably
# needs some fixing in GCP environment before enabling it here, otherwise
# there no fundamental reason it shouldn't work.
ACCEPTED_CLOUD_PROVIDERS = {"docker", "aws"}
ACCEPTED_STORAGE_TYPES = [CloudStorageType.S3]


def supported_storage_types():
    cloud_provider = ACCEPTED_CLOUD_PROVIDERS.intersection(
        {get_cloud_provider()})
    storage_type = get_cloud_storage_type(
        applies_only_on=ACCEPTED_STORAGE_TYPES)
    return list(product(cloud_provider, storage_type))
