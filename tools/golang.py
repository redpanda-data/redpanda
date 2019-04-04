import os

# v's
import shell
import log
from constants import *


def _go_env():
    e = os.environ.copy()
    e["GOPATH"] = GOPATH
    e["GOBIN"] = GOLANG_BUILD_ROOT
    e["CGO_ENABLED"] = "1"
    e["GOOS"] = "linux"
    e["GOARCH"] = "amd64"
    return e


def go_get(package):
    shell.run_subprocess("go get %s" % package, _go_env())


def go_build(path, file, output):
    cmd = "cd %s && go build -a -tags netgo -ldflags '-w' -o %s %s"
    if log.is_debug():
        cmd = "cd %s && go build -a -v -x -tags netgo -ldflags '-w' -o %s %s"
    shell.run_subprocess(cmd % (path, output, file), _go_env())


def go_test(path, packages):
    shell.run_subprocess("cd %s && go test %s" % (path, packages), _go_env())


def get_crlfmt():
    crlfmt_path = "%s/%s" % (GOLANG_BUILD_ROOT, "crlfmt")
    if not os.path.exists(crlfmt_path):
        go_get("github.com/cockroachdb/crlfmt")
    return crlfmt_path
