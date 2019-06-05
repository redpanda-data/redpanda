import sys
import os
import logging

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

from constants import *
import shell
import cpp


def install_deps():
    logger.info("Checking for deps scripts")
    sudo = "sudo -E" if os.getenv("SUDO_USER") == None else ""
    ci = "0" if os.getenv("CI") == None else "1"
    # install our base deps
    shell.run_subprocess("%s bash %s/tools/install-deps.sh" % (sudo, RP_ROOT))
    cpp.get_smf_install_deps()
    logger.info("installing deps")
    shell.run_subprocess("%s CI=%s bash %s/%s" % (sudo, ci, RP_BUILD_ROOT,
                                                  "smf_install_deps.sh"))
