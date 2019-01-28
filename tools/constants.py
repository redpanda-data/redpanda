#!/usr/bin/env python3
import sys
import os
import logging
sys.path.append(os.path.dirname(__file__))

# rp
import git

RP_ROOT = git.get_git_root(relative=os.path.dirname(__file__))
RP_BUILD_ROOT = "%s/build" % RP_ROOT
CPPLINT_URL = "https://raw.githubusercontent.com/google/styleguide/gh-pages/cpplint/cpplint.py"
INSTALL_DEPS_URL = "https://raw.githubusercontent.com/smfrpc/smf/master/install-deps.sh"
CLANG_SOURCE_VERSION = "7.0.0"
