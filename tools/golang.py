import shell
import log
import os


def go_list():
    shell.run_oneline("go list")


def go_get(package):
    shell.run_oneline("go get %s" % package)


def go_build(path, file, output):
    shell.run_subprocess(
        "cd %s && CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -a -v -x -tags netgo -ldflags '-w' -o %s %s"
        % (path, output, file))


def go_test(path, packages):
    shell.run_subprocess("cd %s && go test %s" % (path, packages))


def _get_gopath():
    return shell.run_oneline("go env GOPATH")


def _get_gobinpath():
    return "%s/%s" % (_get_gopath(), "bin")


def get_crlfmt():
    crlfmt_path = "%s/%s" % (_get_gobinpath(), "crlfmt")
    if not os.path.exists(crlfmt_path):
        go_get("github.com/cockroachdb/crlfmt")
    return crlfmt_path
