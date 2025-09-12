#!/usr/bin/env python3

# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
"""
This script sends a signal to a specific thread within a thread group using the `tgkill` syscall.

The `tgkill` system call is used to send signals to specific threads in a thread group by
specifying the thread group ID (tgid), thread ID (tid), and the signal number (sig). See the man
page of `tgkill` for details.

Usage:
    python3 <script_name> <tgid> <tid> <signal>

Arguments:
    tgid (int): The thread group ID of the target thread group.
    tid (int): The thread ID of the target thread within the group.
    sig (int): The signal number to send to the thread.
"""

import ctypes
import os
import platform
import sys


def get_tgkill_syscall_number():
    # Source: https://chromium.googlesource.com/chromiumos/docs/+/master/constants/syscalls.md#cross_arch-numbers
    # syscall name | x86_64 | arm | arm64 | x86
    # tgkill       | 234    | 268 | 131   | 270
    machine = platform.machine()
    if machine == "x86_64":
        return 234
    elif machine == "aarch64":
        return 131
    else:
        raise NotImplementedError(
            f"tgkill syscall number not known for architecture: {machine}"
        )


def tgkill(tgid, tid, sig):
    SYS_tgkill = get_tgkill_syscall_number()
    libc = ctypes.CDLL(None, use_errno=True)
    ret = libc.syscall(SYS_tgkill, tgid, tid, sig)
    if ret != 0:
        errno = ctypes.get_errno()
        raise OSError(errno, os.strerror(errno))
    return ret


def main():
    if len(sys.argv) != 4:
        print("Usage: {} <tgid> <tid> <signal>".format(sys.argv[0]))
        sys.exit(1)
    try:
        tgid = int(sys.argv[1])
        tid = int(sys.argv[2])
        sig = int(sys.argv[3])
    except ValueError:
        print("Error: tgid, tid, and signal must be integers.")
        sys.exit(1)
    try:
        tgkill(tgid, tid, sig)
        print(f"Successfully sent signal {sig} to thread {tid} in group {tgid}.")
    except OSError as e:
        print(f"Error sending signal: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
