import git

VENDOR = "Vectorized Inc."
REDPANDA_NAME = "redpanda"
REDPANDA_DESCRIPTION = 'Redpanda, the fastest queue in the West'
REDPANDA_CATEGORY = "Applications/Misc"
REDPANDA_URL = "https://vectorized.io/product"
LICENSE = 'Proprietary and confidential'
CODENAME = "pandacub"
RELEASE = "1"
REVISION = git.get_sha()
version = git.get_version()

if version == REVISION:
    # when there's not even one tagged commit, git.get_version() returns
    # the SHA1 of this commit, in which case we set VERSION to 0.0
    VERSION = "0.0"
else:
    # in any other case, the returned version is X.Y[-<count>-<sha1>]
    # where [-<count>-<sha1>] is optional.
    version_desc = version.split('-')

    VERSION = version_desc[0]

    if len(version_desc) > 1:
        # since this is not a tagged commit, we set the release ID to be
        # a development one, and we make that dev-N, where N the is the
        # commit count, i.e. the number of commits on the branch from
        # last tagged release.
        RELEASE = "dev-%s" % version_desc[1]
