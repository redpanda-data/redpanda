# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this s
from rptest.services.redpanda import CloudStorageType, get_cloud_provider

# For docker we run only s3 tests because azurite doesn't support
# Data Lake Storage Gen2 which seems to be necessary for iceberg catalog and alike.
# gcp also uses S3 as storage type
# Currently we only run tests in ec2 + S3 combination
# as the query engine deployment was tested only in ec2.
# The credential plumbing (IAM roles, credential providers) probably
# needs some fixing in GCP environment before enabling it here, otherwise
# there no fundamental reason it shouldn't work.
ACCEPTED_VARIANTS = {("docker", CloudStorageType.S3),
                     ("aws", CloudStorageType.S3),
                     ("gcp", CloudStorageType.S3),
                     ("azure", CloudStorageType.ABS)}


def supported_storage_types():
    cloud_provider = get_cloud_provider()
    supported_variants = filter(lambda x: x[0] == cloud_provider,
                                ACCEPTED_VARIANTS)
    supported_storage = map(lambda x: x[1], supported_variants)
    unique = list(set(supported_storage))
    return unique
