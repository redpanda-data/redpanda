import git

VENDOR = "Vectorized Inc."
REDPANDA_NAME = "redpanda"
REDPANDA_DESCRIPTION = 'Redpanda, the fastest queue in the West'
REDPANDA_CATEGORY = "Applications/Misc"
REDPANDA_URL = "https://vectorized.io/product"
LICENSE = 'Proprietary and confidential'
VERSION = "0.1"
RELEASE = "1"
REVISION = git.get_tag_or_ref()
CODENAME = "earlybird"