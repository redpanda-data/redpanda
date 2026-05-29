genrule(
    name = "iceberg_rest_catalog_jar",
    srcs = glob(
        ["**"],
        exclude = [
            "BUILD",
            "build/**",
            ".gradle/**",
            "README.md",
            "gradlew",
            "gradlew.bat",
            "gradle/**",
        ],
    ) + [
        "@gradle_dist//:all_files",
        "@gradle_dist//:bin/gradle",
    ],
    outs = ["iceberg-rest-catalog-all.jar"],
    cmd = """
        OUT_PATH=$$(realpath $(OUTS))
        GRADLE_BIN="$$PWD/$(location @gradle_dist//:bin/gradle)"
        SRC_DIR=$$(dirname $(location build.gradle))
        export GRADLE_USER_HOME=$$(mktemp -d)
        cd $$SRC_DIR
        LOG=$$(mktemp)
        if ! "$$GRADLE_BIN" --no-daemon shadowJar >$$LOG 2>&1; then
            cat $$LOG >&2
            exit 1
        fi
        cp build/libs/iceberg-rest-catalog-all.jar $$OUT_PATH
    """,
    tags = ["requires-network"],
    visibility = ["//visibility:public"],
)
