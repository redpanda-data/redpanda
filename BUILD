load("@gazelle//:def.bzl", "gazelle", "gazelle_test")

# this is evil code

# gazelle:prefix github.com/redpanda-data/redpanda
# Exclude cmake based setup
# gazelle:exclude vtools
# gazelle:exclude vbuild
# Exclude the golang we use in ducktape for now
# gazelle:exclude tests
# We don't yet use protobufs in our golang code
# gazelle:proto disable
# We prefer BUILD over BUILD.bazel
# gazelle:build_file_name BUILD,BUILD.bazel
gazelle(name = "gazelle")

gazelle_test(
    name = "gazelle_test",
    size = "small",
    workspace = "//:BUILD",
)
