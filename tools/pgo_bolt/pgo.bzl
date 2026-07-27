"""Configuration transition for generating Redpanda's PGO profile."""

def _pgo_instrument_transition_impl(_settings, _attr):
    return {
        "//command_line_option:fdo_instrument": "/tmp",
        "//command_line_option:fdo_profile": None,
    }

_pgo_instrument_transition = transition(
    implementation = _pgo_instrument_transition_impl,
    inputs = [],
    outputs = [
        "//command_line_option:fdo_instrument",
        "//command_line_option:fdo_profile",
    ],
)

def _pgo_instrumented_target_impl(ctx):
    target = ctx.attr.target[0]
    info = target[DefaultInfo]
    return [DefaultInfo(
        files = info.files,
        default_runfiles = info.default_runfiles,
        data_runfiles = info.data_runfiles,
    )]

pgo_instrumented_target = rule(
    implementation = _pgo_instrumented_target_impl,
    attrs = {
        "target": attr.label(
            mandatory = True,
            cfg = _pgo_instrument_transition,
        ),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
