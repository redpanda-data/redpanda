#!/usr/bin/env python
# Copyright 2020 Vectorized, Inc.
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
        self.io_delay_ms = 0
        self.io_should_fail = False

    def get_mapped_location(self, path):
        if path.startswith("/"):
            path = path[1:]
        return os.path.join(self.root, path)

    def prologue(self):
        if self.io_delay_ms > 0:
            time.sleep(0.001 * self.io_delay_ms)
        if self.io_should_fail:
            raise FuseOSError(errno.EIO)

    # fs

    def access(self, path, mode):
        self.prologue()
        target = self.get_mapped_location(path)
        if not os.access(target, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        self.prologue()
        target = self.get_mapped_location(path)
        return os.chmod(target, mode)

    def chown(self, path, uid, gid):
        self.prologue()
        target = self.get_mapped_location(path)
        return os.chown(target, uid, gid)

    def getattr(self, path, fh=None):
        self.prologue()
        target = self.get_mapped_location(path)
        lstat = os.lstat(target)
        return dict((key, getattr(lstat, key))
                    for key in ('st_atime', 'st_ctime', 'st_gid', 'st_mode',
                                'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def link(self, target, name):
        self.prologue()
        return os.link(self.get_mapped_location(name),
                       self.get_mapped_location(target))

    def mkdir(self, path, mode):
        self.prologue()
        return os.mkdir(self.get_mapped_location(path), mode)

    def mknod(self, path, mode, dev):
        self.prologue()
        return os.mknod(self.get_mapped_location(path), mode, dev)

    def readdir(self, path, fh):
        self.prologue()
        target = self.get_mapped_location(path)
        if os.path.isdir(target):
            for item in os.listdir(target):
                yield item
        for m in ['.', '..']:
            yield m

    def readlink(self, path):
        self.prologue()
        pathname = os.readlink(self.get_mapped_location(path))
        if pathname.startswith("/"):
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def rename(self, old, new):
        self.prologue()
        return os.rename(self.get_mapped_location(old),
                         self.get_mapped_location(new))

    def rmdir(self, path):
        self.prologue()
        target = self.get_mapped_location(path)
        return os.rmdir(target)

    def statfs(self, path):
        self.prologue()
        target = self.get_mapped_location(path)
        stv = os.statvfs(target)
        return dict((key, getattr(stv, key))
                    for key in ('f_bavail', 'f_bfree', 'f_blocks', 'f_bsize',
                                'f_favail', 'f_ffree', 'f_files', 'f_flag',
                                'f_frsize', 'f_namemax'))

    def symlink(self, name, target):
        self.prologue()
        return os.symlink(target, self.get_mapped_location(name))

    def unlink(self, path):
        self.prologue()
        return os.unlink(self.get_mapped_location(path))

    def utimens(self, path, times=None):
        self.prologue()
        return os.utime(self.get_mapped_location(path), times)

    # file

    def create(self, path, mode, fi=None):
        self.prologue()
        target = self.get_mapped_location(path)
        return os.open(target, os.O_RDWR | os.O_CREAT, mode)

    def flush(self, path, fh):
        self.prologue()
        return os.fsync(fh)

    def fsync(self, path, fdatasync, fh):
        self.prologue()
        return os.fsync(fh)

    def open(self, path, flags):
        self.prologue()
        target = self.get_mapped_location(path)
        return os.open(target, flags)

    def read(self, path, length, offset, fh):
        self.prologue()
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def release(self, path, fh):
        self.prologue()
        return os.close(fh)

    def truncate(self, path, length, fh=None):
        self.prologue()
        if fh:
            os.ftruncate(fh, length)
        else:
            target = self.get_mapped_location(path)
            os.truncate(target, length)

    def write(self, path, buf, offset, fh):
        self.prologue()
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)


mountpoint = sys.argv[2]
target = sys.argv[3]
port = int(sys.argv[1])

app = Flask(__name__)
bindfs = Bindfs(target)


@app.route('/delay/<delay_ms>', methods=['GET'])
def fuse_delay(delay_ms):
    bindfs.io_delay_ms = int(delay_ms)
    return {"status": "ok"}


@app.route('/ruin', methods=['GET'])
def fuse_ruin():
    bindfs.io_should_fail = True
    return {"status": "ok"}


@app.route('/recover', methods=['GET'])
def fuse_recover():
    bindfs.io_should_fail = False
    bindfs.io_delay_ms = 0
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
