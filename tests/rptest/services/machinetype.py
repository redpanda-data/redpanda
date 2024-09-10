from enum import Enum
from dataclasses import dataclass


class MachineTypeName(str, Enum):
    DOCKER = 'docker'

    # AWS X86
    I3EN_LARGE = 'i3en.large'
    I3EN_XLARGE = 'i3en.xlarge'
    I3EN_2XLARGE = 'i3en.2xlarge'
    I3EN_3XLARGE = 'i3en.3xlarge'
    I3EN_6XLARGE = 'i3en.6xlarge'

    # AWS ARM
    IM4GN_LARGE = 'im4gn.large'
    IM4GN_XLARGE = 'im4gn.xlarge'
    IM4GN_8XLARGE = 'im4gn.8xlarge'

    M7GD_LARGE = 'm7gd.large'
    M7GD_XLARGE = 'm7gd.xlarge'
    M7GD_8XLARGE = 'm7gd.8xlarge'

    # Azure X86
    STANDARD_L8S_V3 = 'Standard_L8s_v3'
    STANDARD_L8AS_V3 = 'Standard_L8as_v3'

    # GCP X86
    N2_STANDARD_2 = 'n2-standard-2'
    N2_STANDARD_4 = 'n2-standard-4'
    N2_STANDARD_8 = 'n2-standard-8'
    N2_STANDARD_16 = 'n2-standard-16'
    N2_STANDARD_32 = 'n2-standard-32'
    N2D_STANDARD_2 = 'n2d-standard-2'
    N2D_STANDARD_4 = 'n2d-standard-4'
    N2D_STANDARD_16 = 'n2d-standard-16'
    N2D_STANDARD_32 = 'n2d-standard-32'

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


@dataclass
class MachineTypeConfig:
    num_shards: int
    memory: int


KiB = 1024
MiB = KiB * KiB
GiB = KiB * MiB

MachineTypeConfigs = {
    MachineTypeName.DOCKER:
    MachineTypeConfig(num_shards=2, memory=16 * GiB),

    # AWS X86
    MachineTypeName.I3EN_LARGE:
    MachineTypeConfig(num_shards=1, memory=16 * GiB),
    MachineTypeName.I3EN_XLARGE:
    MachineTypeConfig(num_shards=3, memory=32 * GiB),
    MachineTypeName.I3EN_2XLARGE:
    MachineTypeConfig(num_shards=7, memory=64 * GiB),
    MachineTypeName.I3EN_3XLARGE:
    MachineTypeConfig(num_shards=11, memory=96 * GiB),
    MachineTypeName.I3EN_6XLARGE:
    MachineTypeConfig(num_shards=23, memory=192 * GiB),

    # AWS ARM
    MachineTypeName.IM4GN_LARGE:
    MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.IM4GN_XLARGE:
    MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.IM4GN_8XLARGE:
    MachineTypeConfig(num_shards=31, memory=128 * GiB),
    MachineTypeName.M7GD_LARGE:
    MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.M7GD_XLARGE:
    MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.M7GD_8XLARGE:
    MachineTypeConfig(num_shards=30, memory=128 * GiB),

    # Azure X86
    MachineTypeName.STANDARD_L8S_V3:
    MachineTypeConfig(num_shards=7, memory=64 * GiB),
    MachineTypeName.STANDARD_L8AS_V3:
    MachineTypeConfig(num_shards=7, memory=64 * GiB),

    # GCP X86
    MachineTypeName.N2_STANDARD_2:
    MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.N2_STANDARD_4:
    MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.N2_STANDARD_8:
    MachineTypeConfig(num_shards=7, memory=32 * GiB),
    MachineTypeName.N2_STANDARD_16:
    MachineTypeConfig(num_shards=15, memory=64 * GiB),
    MachineTypeName.N2_STANDARD_32:
    MachineTypeConfig(num_shards=31, memory=128 * GiB),
    MachineTypeName.N2D_STANDARD_2:
    MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.N2D_STANDARD_4:
    MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.N2D_STANDARD_16:
    MachineTypeConfig(num_shards=15, memory=64 * GiB),
    MachineTypeName.N2D_STANDARD_32:
    MachineTypeConfig(num_shards=31, memory=128 * GiB),
}


def get_machine_info(machine_type: str) -> MachineTypeConfig:
    return MachineTypeConfigs[MachineTypeName(machine_type)]
