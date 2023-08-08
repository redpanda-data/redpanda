import os
import yaml

from enum import Enum
from typing import Optional

rp_profiles_path = os.path.join(os.path.dirname(__file__),
                                "rp_config_profiles")
tiers_config_filename = os.path.join(rp_profiles_path,
                                     "redpanda.cloud-tiers-config.yml")


def load_tier_profiles():
    with open(os.path.join(os.path.dirname(__file__), tiers_config_filename),
              "r") as f:
        # TODO: validate input
        _profiles = yaml.safe_load(f)['config_profiles']
    return _profiles


class CloudTierName(Enum):
    AWS_1 = 'tier-1-aws'
    AWS_2 = 'tier-2-aws'
    AWS_3 = 'tier-3-aws'
    AWS_4 = 'tier-4-aws'
    AWS_5 = 'tier-5-aws'
    GCP_1 = 'tier-1-gcp'
    GCP_2 = 'tier-2-gcp'
    GCP_3 = 'tier-3-gcp'
    GCP_4 = 'tier-4-gcp'
    GCP_5 = 'tier-5-gcp'

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class AdvertisedTierConfig:
    def __init__(self, ingress_rate: float, egress_rate: float,
                 num_brokers: int, segment_size: int, cloud_cache_size: int,
                 partitions_min: int, partitions_max: int,
                 connections_limit: Optional[int],
                 memory_per_broker: int) -> None:
        self.ingress_rate = int(ingress_rate)
        self.egress_rate = int(egress_rate)
        self.num_brokers = num_brokers
        self.segment_size = segment_size
        self.cloud_cache_size = cloud_cache_size
        self.partitions_min = partitions_min
        self.partitions_max = partitions_max
        self.connections_limit = connections_limit
        self.memory_per_broker = memory_per_broker


kiB = 1024
MiB = kiB * kiB
GiB = MiB * kiB

# yapf: disable
AdvertisedTierConfigs = {
    #   ingress|          segment size|       partitions max|
    #             egress|       cloud cache size|connections # limit|
    #           # of brokers|           partitions min|           memory per broker|
    CloudTierName.AWS_1: AdvertisedTierConfig(
         25*MiB,  75*MiB,  3,  512*MiB,  300*GiB,   20, 1000,  1500, 16*GiB
    ),
    CloudTierName.AWS_2: AdvertisedTierConfig(
         50*MiB, 150*MiB,  3,  512*MiB,  500*GiB,   50, 2000,  3750, 32*GiB
    ),
    CloudTierName.AWS_3: AdvertisedTierConfig(
        100*MiB, 200*MiB,  6,  512*MiB,  500*GiB,  100, 5000,  7500, 32*GiB
    ),
    CloudTierName.AWS_4: AdvertisedTierConfig(
        200*MiB, 400*MiB,  6,    1*GiB, 1000*GiB,  100, 5000, 15000, 96*GiB
    ),
    CloudTierName.AWS_5: AdvertisedTierConfig(
        300*MiB, 600*MiB,  9,    1*GiB, 1000*GiB,  150, 7500, 22500, 96*GiB
    ),
    CloudTierName.GCP_1: AdvertisedTierConfig(
         25*MiB,  60*MiB,  3,  512*MiB,  150*GiB,   20,  500,  1500,  8*GiB
    ),
    CloudTierName.GCP_2: AdvertisedTierConfig(
         50*MiB, 150*MiB,  3,  512*MiB,  300*GiB,   50, 1000,  3750, 32*GiB
    ),
    CloudTierName.GCP_3: AdvertisedTierConfig(
        100*MiB, 200*MiB,  6,  512*MiB,  320*GiB,  100, 3000,  7500, 32*GiB
    ),
    CloudTierName.GCP_4: AdvertisedTierConfig(
        200*MiB, 400*MiB,  9,  512*MiB,  350*GiB,  100, 5000, 15000, 32*GiB
    ),
    CloudTierName.GCP_5: AdvertisedTierConfig(
        400*MiB, 600*MiB, 12,    1*GiB,  750*GiB,  100, 7500, 22500, 32*GiB
    ),
}
# yapf: enable
