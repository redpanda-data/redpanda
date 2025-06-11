#! /usr/bin/env python3

from dataclasses import dataclass
import json
import logging
import os
import subprocess
import sys
import traceback

logger = logging.getLogger('cc_linker_gen')


@dataclass
class Action:
    mnemonic: str
    command: [str]


@dataclass
class LinkedObject:
    filename: str
    arguments: [str]

    def as_dict(self):
        return {"file": self.filename, "arguments": self.arguments}


def get_field(data, field, ctx):
    if field not in data:
        raise RuntimeError(f"Missing required field '{field}' from '{ctx}'")
    return data[field]


def write(data, output_file):
    with open(output_file, "w") as file:
        json.dump(data, file, indent=2)


def execute_process(command: [str]):
    logger.debug(f"Execute {' '.join(command)}")
    proc = subprocess.run(command, capture_output=True)
    proc.check_returncode()
    return proc.stdout, proc.stderr


def query_linker_commands(extra_args):
    # This command is based on the command used to generate
    # compile_commands.json, repurposed for linking flags
    cmd = [
        "bazel",
        "aquery",
        "mnemonic('CppLink', //... union @seastar//...)",
        "--output=jsonproto",
        "--include_artifacts=false",
        "--ui_event_filters=-info",
        "--noshow_progress",
        "--features=-compiler_param_file",
        "--host_features=-compiler_param_file",
        "--features=-layering_check",
        "--host_features=-layering_check",
        "--features=-parse_headers",
        "--host_features=-parse_headers",
    ]
    if len(extra_args) != 0:
        cmd.extend(extra_args)
    raw_data, _ = execute_process(cmd)
    return json.loads(raw_data)


def parse_action(data):
    ctx = "action json entry"
    action = Action(mnemonic=get_field(data, "mnemonic", ctx),
                    command=get_field(data, "arguments", ctx))

    if action.mnemonic == "":
        raise RuntimeError(f"Empty 'mnemonic' entry.\nAction '{action}'")
    if len(action.command) == 0:
        raise RuntimeError(f"Empty 'arguments' entry.\nAction '{action}'")

    return action


def to_linked_object(action: Action):
    if os.path.basename(action.command[0]) != "cc_wrapper.sh":
        raise RuntimeError(
            f"Invalid 'arguments' executable. Expected 'cc_wrapper'.\nAction: '{action}'"
        )

    try:
        output_index = action.command.index("-o")
    except ValueError:
        raise RuntimeError(
            f"Missing '-o FILENAME' from arguments.\nAction: '{action}'")
    if output_index == len(action.command) - 1:
        raise RuntimeError(
            f"Missing FILENAME from arguments after '-o'.\nAction: '{action}'")

    filename = action.command[output_index + 1]
    # Drop executable name and output file arguemnts
    arguments = action.command[1:output_index] + action.command[output_index +
                                                                2:]

    return LinkedObject(filename, arguments)


def reformat_linker_commands(link_cmds):
    actions = [
        parse_action(action)
        for action in get_field(link_cmds, "actions", "bazel aquery output")
    ]
    link_actions = [
        action for action in actions if action.mnemonic == "CppLink"
    ]
    return [to_linked_object(action).as_dict() for action in link_actions]


def generate_linker_commands(extra_args):
    logger.debug(f"Extra_args: {' '.join(extra_args)}")
    link_cmds = query_linker_commands(extra_args)
    formatted_cmds = reformat_linker_commands(link_cmds)
    write(formatted_cmds, "link_commands.json")
    pass


def switch_cwd_to_workspace():
    root = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if not root:
        RuntimeError(
            "BUILD_WORKSPACE_DIRECTORY was not found in the environment. Make sure to invoke this with `bazel run`"
        )
    os.chdir(root)


def main():
    handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(handler)
    args = sys.argv[1:]
    logger.info(f"args: {' '.join(args)}\n")
    try:
        switch_cwd_to_workspace()
        generate_linker_commands(args)
    except RuntimeError as e:
        logger.error(f"Failed to generate linker commands: {e}")
        return 1
    except subprocess.CalledProcessError as e:
        logger.error(
            f'Failed to generate linker commands:\n{e.stderr}\n{e.stdout}')
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f'Exiting on uncaught exception: {e}')
        traceback.print_exc()
        sys.exit(1)
