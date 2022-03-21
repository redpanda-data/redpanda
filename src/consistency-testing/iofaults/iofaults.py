#!/usr/bin/env python
# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import sys
import errno
from flask import Flask, json
import time
import threading
from fuse import FUSE, FuseOSError, Operations


class Bindfs(Operations):
    def __init__(self, root):
        self.root = root

        self.io_op_delay_ms = {
            "access": 0,
            "chmod": 0,
            "chown": 0,
            "getattr": 0,
            "link": 0,
            "mkdir": 0,
            "mknod": 0,
            "readdir": 0,
            "readlink": 0,
            "rename": 0,
            "rmdir": 0,
            "statfs": 0,
            "symlink": 0,
            "unlink": 0,
            "utimens": 0,
            "create": 0,
            "flush": 0,
            "fsync": 0,
            "open": 0,
            "read": 0,
            "release": 0,
            "truncate": 0,
            "write": 0
        }

        self.io_op_should_fail = {
            "access": False,
            "chmod": False,
            "chown": False,
            "getattr": False,
            "link": False,
            "mkdir": False,
            "mknod": False,
            "readdir": False,
            "readlink": False,
            "rename": False,
            "rmdir": False,
            "statfs": False,
            "symlink": False,
            "unlink": False,
            "utimens": False,
            "create": False,
            "flush": False,
            "fsync": False,
            "open": False,
            "read": False,
            "release": False,
            "truncate": False,
            "write": False
        }

    def get_mapped_location(self, path):
        if path.startswith("/"):
            path = path[1:]
        return os.path.join(self.root, path)

    def prologue(self, op_name):
        if self.io_op_delay_ms[op_name] > 0:
            time.sleep(0.001 * self.io_op_delay_ms[op_name])
        if self.io_op_should_fail[op_name]:
            raise FuseOSError(errno.EIO)

    # fs

    def access(self, path, mode):
        self.prologue("access")
        target = self.get_mapped_location(path)
        if not os.access(target, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        self.prologue("chmod")
        target = self.get_mapped_location(path)
        return os.chmod(target, mode)

    def chown(self, path, uid, gid):
        self.prologue("chown")
        target = self.get_mapped_location(path)
        return os.chown(target, uid, gid)

    def getattr(self, path, fh=None):
        self.prologue("getattr")
        target = self.get_mapped_location(path)
        lstat = os.lstat(target)
        return dict((key, getattr(lstat, key))
                    for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode',
                                'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def link(self, target, name):
        self.prologue("link")
        return os.link(self.get_mapped_location(name),
                       self.get_mapped_location(target))

    def mkdir(self, path, mode):
        self.prologue("mkdir")
        return os.mkdir(self.get_mapped_location(path), mode)

    def mknod(self, path, mode, dev):
        self.prologue("mknod")
        return os.mknod(self.get_mapped_location(path), mode, dev)

    def readdir(self, path, fh):
        self.prologue("readdir")
        target = self.get_mapped_location(path)
        if os.path.isdir(target):
            for item in os.listdir(target):
                yield item
        for m in ['.', '..']:
            yield m

    def readlink(self, path):
        self.prologue("readlink")
        pathname = os.readlink(self.get_mapped_location(path))
        if pathname.startswith("/"):
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def rename(self, old, new):
        self.prologue("rename")
        return os.rename(self.get_mapped_location(old),
                         self.get_mapped_location(new))

    def rmdir(self, path):
        self.prologue("rmdir")
        target = self.get_mapped_location(path)
        return os.rmdir(target)

    def statfs(self, path):
        self.prologue("statfs")
        target = self.get_mapped_location(path)
        stv = os.statvfs(target)
        return dict((key, getattr(stv, key))
                    for key in ('f_bavail', 'f_bfree', 'f_blocks', 'f_bsize',
                                'f_favail', 'f_ffree', 'f_files', 'f_flag',
                                'f_frsize', 'f_namemax'))

    def symlink(self, name, target):
        self.prologue("symlink")
        return os.symlink(target, self.get_mapped_location(name))

    def unlink(self, path):
        self.prologue("unlink")
        return os.unlink(self.get_mapped_location(path))

    def utimens(self, path, times=None):
        self.prologue("utimens")
        return os.utime(self.get_mapped_location(path), times)

    # file

    def create(self, path, mode, fi=None):
        self.prologue("create")
        target = self.get_mapped_location(path)
        return os.open(target, os.O_RDWR | os.O_CREAT, mode)

    def flush(self, path, fh):
        self.prologue("flush")
        return os.fsync(fh)

    def fsync(self, path, fdatasync, fh):
        self.prologue("fsync")
        return os.fsync(fh)

    def open(self, path, flags):
        self.prologue("open")
        target = self.get_mapped_location(path)
        return os.open(target, flags)

    def read(self, path, length, offset, fh):
        self.prologue("read")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def release(self, path, fh):
        self.prologue("release")
        return os.close(fh)

    def truncate(self, path, length, fh=None):
        self.prologue("truncate")
        if fh:
            os.ftruncate(fh, length)
        else:
            target = self.get_mapped_location(path)
            os.truncate(target, length)

    def write(self, path, buf, offset, fh):
        self.prologue("write")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)


mountpoint = sys.argv[2]
target = sys.argv[3]
port = int(sys.argv[1])

app = Flask(__name__)
bindfs = Bindfs(target)


@app.route('/delay/<op_name>/<delay_ms>', methods=['GET'])
def fuse_delay(op_name, delay_ms):
    if op_name == "all":
        for key in bindfs.io_op_delay_ms.keys():
            bindfs.io_op_delay_ms[key] = int(delay_ms)
        return {"status": "ok"}
    elif op_name in bindfs.io_op_delay_ms:
        bindfs.io_op_delay_ms[op_name] = int(delay_ms)
        return {"status": "ok"}
    else:
        return {"status": "fail", "error": "op " + op_name + " isn't found"}


@app.route('/ruin/<op_name>', methods=['GET'])
def fuse_ruin(op_name):
    if op_name == "all":
        for key in bindfs.io_op_should_fail.keys():
            bindfs.io_op_should_fail[key] = True
        return {"status": "ok"}
    elif op_name in bindfs.io_op_delay_ms:
        bindfs.io_op_should_fail[op_name] = True
        return {"status": "ok"}
    else:
        return {"status": "fail", "error": "op " + op_name + " isn't found"}


@app.route('/recover', methods=['GET'])
def fuse_recover():
    for key in bindfs.io_op_should_fail.keys():
        bindfs.io_op_should_fail[key] = False
    for key in bindfs.io_op_delay_ms.keys():
        bindfs.io_op_delay_ms[key] = 0
    return {"status": "ok"}


@app.route('/status', methods=['GET'])
def status():
    return {"status": "ok"}


fuse_thread = threading.Thread(
    target=lambda: FUSE(bindfs, mountpoint, nothreads=True, foreground=True))
fuse_thread.start()

print("Successfully started iofaults!")

app.run(host='0.0.0.0', port=port, use_reloader=False)
fuse_thread.join()
