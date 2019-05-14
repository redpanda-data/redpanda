#!/usr/bin/env python3
import sys
import os
import logging
sys.path.append(os.path.dirname(__file__))

# rp
import git

RP_ROOT = git.get_git_root(relative=os.path.dirname(__file__))
RP_BUILD_ROOT = "%s/build" % RP_ROOT
GOPATH = "%s/gopath" % RP_BUILD_ROOT
GOLANG_ROOT = "%s/src/go/pkg" % RP_ROOT
GOLANG_CMDS_ROOT = "%s/cmd" % GOLANG_ROOT
GOLANG_BUILD_ROOT = "%s/go/bin" % RP_BUILD_ROOT
GOLANG_COMPLILER_ROOT = "%s/go" % RP_BUILD_ROOT
GOLANG_VERSION = "1.12.3"
GOLANG_COMPILER = "%s/%s/go/bin/go" % (GOLANG_COMPLILER_ROOT, GOLANG_VERSION)
CPPLINT_URL = "https://raw.githubusercontent.com/google/styleguide/gh-pages/cpplint/cpplint.py"
INSTALL_DEPS_URL = "https://raw.githubusercontent.com/smfrpc/smf/master/install-deps.sh"
CLANG_SOURCE_VERSION = "7.0.1"

LLVM_REF='llvmorg-8.0.0'
LLVM_MD5='56d611480b48aee351c13e113f7722e6'
