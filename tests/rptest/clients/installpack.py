import json
import tarfile
import tempfile
from typing import Any

import requests

from rptest.util import not_none


class InstallPackClient:
    def __init__(self, baseURLTmpl: str, authType: str, auth: str) -> None:
        self._baseURLTmpl = baseURLTmpl
        self._authType = authType
        self._auth = auth

    def getInstallPack(self, version: str) -> Any:
        headers = {"Authorization": "{} {}".format(self._authType, self._auth)}
        with requests.get(
            self._baseURLTmpl.format(install_pack_ver=version),
            headers=headers,
            stream=True,
        ) as r:
            if r.status_code != requests.status_codes.codes.ok:
                r.raise_for_status()
            with tempfile.NamedTemporaryFile() as tmp_file:
                tmp_file.write(r.raw.read())
                tmp_file.flush()

                with tarfile.open(tmp_file.name, "r:gz") as tfile:
                    return json.load(not_none(tfile.extractfile("install-pack.json")))
