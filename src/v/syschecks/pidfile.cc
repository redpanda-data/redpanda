// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "base/vassert.h"
#include "syschecks/syschecks.h"

#include <seastar/core/future.hh>
#include <seastar/core/posix.hh>
#include <seastar/core/sstring.hh>

#include <filesystem>
#include <memory>

namespace syschecks {

struct pid_file {
    std::filesystem::path path;
    std::optional<ss::file_desc> fd;
    dev_t dev;
    ino_t ino;

    pid_file(std::filesystem::path path)
      : path(std::move(path)) {
        create();
    }

    ~pid_file() { remove(); }

    void create();
    void remove();
};

/*
 * Attempt to remove the pid file if it appears to still belong to us. This
 * should only be called on shutdown, so we if we encounter an error we log the
 * issue but don't throw an exception.
 */
void pid_file::remove() {
    if (!fd) {
        return;
    }

    struct stat st;
    int ret = ::stat(path.c_str(), &st);
    if (ret < 0) {
        checklog.warn(
          "Failed to stat while removing pid file {}:{}", ret, path);
        return;
    }

    if (dev != st.st_dev || ino != st.st_ino) {
        checklog.warn(
          "The pid file has changed unexpectedly. Skipping removal {}", path);
        return;
    }

    ret = ::lseek(fd->get(), 0, SEEK_SET);
    if (ret < 0) {
        checklog.warn("Failed to seek pid file whle removing {}:{}", ret, path);
        return;
    }

    char buf[32];
    memset(buf, 0, sizeof(buf));

    try {
        fd->read(buf, sizeof(buf));
    } catch (...) {
        checklog.warn(
          "Failed to read pid file {}:{}", path, std::current_exception());
        return;
    }

    try {
        fd->close();
    } catch (...) {
        // that's ok
    }

    int pid = atoi(buf);
    if (getpid() != pid) {
        checklog.warn(
          "The pid file contains {} != current pid {}. Skipping removal {}",
          pid,
          getpid(),
          path);
        return;
    }

    ret = ::unlink(path.c_str());
    if (ret < 0) {
        checklog.warn("Failed to unlink pid file {}:{}", ret, path);
    }
}

void pid_file::create() {
    // open or create
    checklog.info("Writing pid file {}", path);
    fd = ss::file_desc::open(path.string(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);

    // record file identify
    struct stat st;
    int ret = ::stat(path.c_str(), &st);
    ss::throw_system_error_on(ret < 0, "stat");
    dev = st.st_dev;
    ino = st.st_ino;

    // try to lock
    struct flock lock = {
      .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0};
    ret = ::fcntl(fd->get(), F_SETLK, &lock);
    if (ret < 0) {
        if (errno == EAGAIN || errno == EACCES) {
            throw std::runtime_error("failed to lock pidfile. already locked");
        } else {
            ss::throw_system_error_on(true, "fcntl");
        }
    }

    fd->truncate(0);

    // write pid
    char buf[32];
    int len = snprintf(buf, sizeof(buf), "%d\n", getpid());
    vassert(len > 0, "error filling pid buffer");

    size_t left = len;
    size_t offset = 0;
    while (left) {
        auto written = fd->write(buf + offset, left);
        vassert(written, "fd is not open as non-blocking");
        offset += *written;
        left -= *written;
    }
}

static std::unique_ptr<pid_file> pf;

void pidfile_delete() { pf.reset(); }

void pidfile_create(std::filesystem::path path) {
    vassert(!pf, "pidfile already created: {}", path);
    pf = std::make_unique<pid_file>(std::move(path));
    atexit(pidfile_delete);
}

} // namespace syschecks
