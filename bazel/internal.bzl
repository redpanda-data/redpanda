def redpanda_copts():
    # TODO Bazel prefers -iquote "path" style includes in many cases. However,
    # our source tree uses bracket <path> style for dependencies. We need a way
    # to bridge this gap until we decide to fully switch over to Bazel at which
    # point this hack can be removed. Many deps lists in the tree will probably
    # need to be updated to include abseil explicitly when this is removed.
    copts = []
    copts.append("-Iexternal/abseil-cpp~")
    return copts
