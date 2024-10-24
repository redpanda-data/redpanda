#
# This build is adapted from the xxhash build in bazel central repository, but
# updated with `includes` to include `.` so that bracket-style includes will
# work in Redpanda. The cmake build in the xxhash source tree looks similar.
#
# TODO(bazel) once cmake is removed from Redpanda we can switch everything over to the
# default quoted include paths and remove the `includes` parameter here.
#

cc_library(
    name = "xxhash",
    srcs = [
        "xxhash.c",
    ],
    hdrs = [
        "xxh3.h",
        "xxhash.h",
    ],
    includes = [
        ".",
    ],
    visibility = ["//visibility:public"],
)
