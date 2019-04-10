import sys
import struct
import os
import platform


def execute_for_os(**kwargs):
    """Executes given OS specific actions
    :param linux: (optional) action to execute on all linux distros
    :param darwin (optional) action to execute on OS X
    :param freebsd (optional) action to execute on FreeBSD
    """
    for platform, action in kwargs.items():
        if sys.platform.startswith(platform):
            action()


def execute_for_arch(**kwargs):
    """Executes given actions conditionally for x86 and x64 architecture
    :param x86: (optional) action to execute in 32-bit environment
    :param x64: (optional) action to execute in 64-bit environment
    :param aarm32: (optional) action to execute in 32-bit ARM environment
    :param aarm64: (optional) action to execute in 64-bit ARM environment
    """
    arch = _get_arch()
    if arch in kwargs:
        kwargs[arch]()


def _get_arch():
    machine = os.uname().machine
    if 'arm' in machine:
        if 'armv7' in machine or 'armv6' in machine:
            return 'aarm32'
        else:
            return 'aarm64'
    else:
        return 'x64' if struct.calcsize("P") * 8 == 64 else 'x86'
