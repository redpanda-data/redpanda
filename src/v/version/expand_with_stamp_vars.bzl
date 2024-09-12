"""
A rule to expand a file using variables from the workspace status command.

The template is a python format, so we can expand things like {STABLE_GIT_COMMIT}
to the current commmit. Provide a `defaults_file` to file in defaults if the build
is not stamped (ie. dev builds).
"""

def _expand_with_stamp_vars(ctx):
    ctx.actions.run(
        outputs = [ctx.outputs.out],
        inputs = [ctx.file.defaults_file, ctx.file.template, ctx.info_file],
        tools = [ctx.executable._tool],
        executable = ctx.executable._tool,
        arguments = [
            "--template",
            ctx.file.template.path,
            "--variables",
            ctx.file.defaults_file.path,
            ctx.info_file.path,
            "--output",
            ctx.outputs.out.path,
        ],
        mnemonic = "ExpandStampVars",
        use_default_shell_env = False,
    )
    return DefaultInfo(
        files = depset([ctx.outputs.out]),
        runfiles = ctx.runfiles(files = [ctx.outputs.out]),
    )

expand_with_stamp_vars = rule(
    implementation = _expand_with_stamp_vars,
    attrs = {
        "_tool": attr.label(
            executable = True,
            allow_files = True,
            cfg = "exec",
            default = Label("//src/v/version:stamper"),
        ),
        "template": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "defaults_file": attr.label(
            allow_single_file = True,
            mandatory = True,
        ),
        "out": attr.output(
            mandatory = True,
        ),
    },
)
