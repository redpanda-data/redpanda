from dataclasses import dataclass
from enum import Enum


class MachineTypeName(str, Enum):
    DOCKER = "docker"

    # AWS X86
    I3EN_LARGE = "i3en.large"
    I3EN_XLARGE = "i3en.xlarge"
    I3EN_2XLARGE = "i3en.2xlarge"
    I3EN_3XLARGE = "i3en.3xlarge"
    I3EN_6XLARGE = "i3en.6xlarge"

    # AWS ARM
    IM4GN_LARGE = "im4gn.large"
    IM4GN_XLARGE = "im4gn.xlarge"
    IM4GN_8XLARGE = "im4gn.8xlarge"

    M7GD_LARGE = "m7gd.large"
    M7GD_XLARGE = "m7gd.xlarge"
    M7GD_8XLARGE = "m7gd.8xlarge"

    # Azure X86
    STANDARD_L8S_V3 = "Standard_L8s_v3"
    STANDARD_L8AS_V3 = "Standard_L8as_v3"
    STANDARD_D2D_V5 = "Standard_D2d_v5"
    STANDARD_D4D_V5 = "Standard_D4d_v5"
    STANDARD_D32D_V5 = "Standard_D32d_v5"

    # GCP X86
    N2_STANDARD_2 = "n2-standard-2"
    N2_STANDARD_4 = "n2-standard-4"
    N2_STANDARD_8 = "n2-standard-8"
    N2_STANDARD_16 = "n2-standard-16"
    N2_STANDARD_32 = "n2-standard-32"
    N2D_STANDARD_2 = "n2d-standard-2"
    N2D_STANDARD_4 = "n2d-standard-4"
    N2D_STANDARD_16 = "n2d-standard-16"
    N2D_STANDARD_32 = "n2d-standard-32"

    # GCP X86 (C4, Intel - v3 tiers)
    C4_STANDARD_4_LSSD = "c4-standard-4-lssd"
    C4_STANDARD_8_LSSD = "c4-standard-8-lssd"
    C4_STANDARD_16_LSSD = "c4-standard-16-lssd"
    C4_STANDARD_24_LSSD = "c4-standard-24-lssd"
    C4_STANDARD_32_LSSD = "c4-standard-32-lssd"
    C4_STANDARD_48_LSSD = "c4-standard-48-lssd"
    C4_STANDARD_96_LSSD = "c4-standard-96-lssd"
    C4_STANDARD_144_LSSD = "c4-standard-144-lssd"
    C4_STANDARD_192_LSSD = "c4-standard-192-lssd"
    C4_STANDARD_288_LSSD = "c4-standard-288-lssd"

    # GCP X86 (C4D, AMD)
    C4D_STANDARD_8_LSSD = "c4d-standard-8-lssd"
    C4D_STANDARD_16_LSSD = "c4d-standard-16-lssd"
    C4D_STANDARD_32_LSSD = "c4d-standard-32-lssd"
    C4D_STANDARD_48_LSSD = "c4d-standard-48-lssd"
    C4D_STANDARD_64_LSSD = "c4d-standard-64-lssd"
    C4D_STANDARD_96_LSSD = "c4d-standard-96-lssd"
    C4D_STANDARD_192_LSSD = "c4d-standard-192-lssd"
    C4D_STANDARD_384_LSSD = "c4d-standard-384-lssd"

    # GCP ARM (C4A, Axion)
    C4A_STANDARD_4_LSSD = "c4a-standard-4-lssd"
    C4A_STANDARD_8_LSSD = "c4a-standard-8-lssd"
    C4A_STANDARD_16_LSSD = "c4a-standard-16-lssd"
    C4A_STANDARD_32_LSSD = "c4a-standard-32-lssd"
    C4A_STANDARD_48_LSSD = "c4a-standard-48-lssd"
    C4A_STANDARD_64_LSSD = "c4a-standard-64-lssd"
    C4A_STANDARD_72_LSSD = "c4a-standard-72-lssd"

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
    MachineTypeName.DOCKER: MachineTypeConfig(num_shards=2, memory=16 * GiB),
    # AWS X86
    MachineTypeName.I3EN_LARGE: MachineTypeConfig(num_shards=1, memory=16 * GiB),
    MachineTypeName.I3EN_XLARGE: MachineTypeConfig(num_shards=3, memory=32 * GiB),
    MachineTypeName.I3EN_2XLARGE: MachineTypeConfig(num_shards=7, memory=64 * GiB),
    MachineTypeName.I3EN_3XLARGE: MachineTypeConfig(num_shards=11, memory=96 * GiB),
    MachineTypeName.I3EN_6XLARGE: MachineTypeConfig(num_shards=23, memory=192 * GiB),
    # AWS ARM
    MachineTypeName.IM4GN_LARGE: MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.IM4GN_XLARGE: MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.IM4GN_8XLARGE: MachineTypeConfig(num_shards=31, memory=128 * GiB),
    MachineTypeName.M7GD_LARGE: MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.M7GD_XLARGE: MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.M7GD_8XLARGE: MachineTypeConfig(num_shards=30, memory=128 * GiB),
    # Azure X86
    MachineTypeName.STANDARD_L8S_V3: MachineTypeConfig(num_shards=7, memory=64 * GiB),
    MachineTypeName.STANDARD_L8AS_V3: MachineTypeConfig(num_shards=7, memory=64 * GiB),
    MachineTypeName.STANDARD_D2D_V5: MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.STANDARD_D4D_V5: MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.STANDARD_D32D_V5: MachineTypeConfig(
        num_shards=30, memory=128 * GiB
    ),
    # GCP X86
    MachineTypeName.N2_STANDARD_2: MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.N2_STANDARD_4: MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.N2_STANDARD_8: MachineTypeConfig(num_shards=7, memory=32 * GiB),
    MachineTypeName.N2_STANDARD_16: MachineTypeConfig(num_shards=15, memory=64 * GiB),
    MachineTypeName.N2_STANDARD_32: MachineTypeConfig(num_shards=31, memory=128 * GiB),
    MachineTypeName.N2D_STANDARD_2: MachineTypeConfig(num_shards=1, memory=8 * GiB),
    MachineTypeName.N2D_STANDARD_4: MachineTypeConfig(num_shards=3, memory=16 * GiB),
    MachineTypeName.N2D_STANDARD_16: MachineTypeConfig(num_shards=15, memory=64 * GiB),
    MachineTypeName.N2D_STANDARD_32: MachineTypeConfig(num_shards=31, memory=128 * GiB),
    # GCP X86 (C4, Intel - v3 tiers): num_shards = vCPU - 1, reserving one core
    # for housekeeping (matches the N2/N2D convention)
    MachineTypeName.C4_STANDARD_4_LSSD: MachineTypeConfig(
        num_shards=3, memory=15 * GiB
    ),
    MachineTypeName.C4_STANDARD_8_LSSD: MachineTypeConfig(
        num_shards=7, memory=30 * GiB
    ),
    MachineTypeName.C4_STANDARD_16_LSSD: MachineTypeConfig(
        num_shards=15, memory=60 * GiB
    ),
    MachineTypeName.C4_STANDARD_24_LSSD: MachineTypeConfig(
        num_shards=23, memory=90 * GiB
    ),
    MachineTypeName.C4_STANDARD_32_LSSD: MachineTypeConfig(
        num_shards=31, memory=120 * GiB
    ),
    MachineTypeName.C4_STANDARD_48_LSSD: MachineTypeConfig(
        num_shards=47, memory=180 * GiB
    ),
    MachineTypeName.C4_STANDARD_96_LSSD: MachineTypeConfig(
        num_shards=95, memory=360 * GiB
    ),
    MachineTypeName.C4_STANDARD_144_LSSD: MachineTypeConfig(
        num_shards=143, memory=540 * GiB
    ),
    MachineTypeName.C4_STANDARD_192_LSSD: MachineTypeConfig(
        num_shards=191, memory=720 * GiB
    ),
    MachineTypeName.C4_STANDARD_288_LSSD: MachineTypeConfig(
        num_shards=287, memory=1080 * GiB
    ),
    # GCP X86 (C4D, AMD)
    MachineTypeName.C4D_STANDARD_8_LSSD: MachineTypeConfig(
        num_shards=7, memory=31 * GiB
    ),
    MachineTypeName.C4D_STANDARD_16_LSSD: MachineTypeConfig(
        num_shards=15, memory=62 * GiB
    ),
    MachineTypeName.C4D_STANDARD_32_LSSD: MachineTypeConfig(
        num_shards=31, memory=124 * GiB
    ),
    MachineTypeName.C4D_STANDARD_48_LSSD: MachineTypeConfig(
        num_shards=47, memory=186 * GiB
    ),
    MachineTypeName.C4D_STANDARD_64_LSSD: MachineTypeConfig(
        num_shards=63, memory=248 * GiB
    ),
    MachineTypeName.C4D_STANDARD_96_LSSD: MachineTypeConfig(
        num_shards=95, memory=372 * GiB
    ),
    MachineTypeName.C4D_STANDARD_192_LSSD: MachineTypeConfig(
        num_shards=191, memory=744 * GiB
    ),
    MachineTypeName.C4D_STANDARD_384_LSSD: MachineTypeConfig(
        num_shards=383, memory=1488 * GiB
    ),
    # GCP ARM (C4A, Axion)
    MachineTypeName.C4A_STANDARD_4_LSSD: MachineTypeConfig(
        num_shards=3, memory=16 * GiB
    ),
    MachineTypeName.C4A_STANDARD_8_LSSD: MachineTypeConfig(
        num_shards=7, memory=32 * GiB
    ),
    MachineTypeName.C4A_STANDARD_16_LSSD: MachineTypeConfig(
        num_shards=15, memory=64 * GiB
    ),
    MachineTypeName.C4A_STANDARD_32_LSSD: MachineTypeConfig(
        num_shards=31, memory=128 * GiB
    ),
    MachineTypeName.C4A_STANDARD_48_LSSD: MachineTypeConfig(
        num_shards=47, memory=192 * GiB
    ),
    MachineTypeName.C4A_STANDARD_64_LSSD: MachineTypeConfig(
        num_shards=63, memory=256 * GiB
    ),
    MachineTypeName.C4A_STANDARD_72_LSSD: MachineTypeConfig(
        num_shards=71, memory=288 * GiB
    ),
}


def get_machine_info(machine_type: str) -> MachineTypeConfig:
    return MachineTypeConfigs[MachineTypeName(machine_type)]
