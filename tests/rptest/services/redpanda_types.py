import collections

# Basic types shared between various redpanda service and test modes.

SaslCredentials = collections.namedtuple("SaslCredentials",
                                         ["username", "password", "algorithm"])
