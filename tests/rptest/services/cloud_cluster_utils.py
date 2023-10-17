from rptest.clients.rpk import RpkTool


class FakePanda:
    logger = None

    def __init__(self, context, log):
        self._context = context
        self.logger = log


class CloudClusterUtils:
    def __init__(self, context, logger, infra_id, infra_secret, provider):
        # Create fake redpanda class with logger only
        self.fake_panda = FakePanda(context, logger)
        # Create rpk to use several functions that is isolated
        # from actual redpanda service
        self.rpk = RpkTool(self.fake_panda)
        self.logger = logger
        self.provider = provider.lower()
        self.env = {
            "RPK_CLOUD_SKIP_VERSION_CHECK": "True",
            "RPK_CLOUD_URL": "https://cloud-api.ppd.cloud.redpanda.com",
            "RPK_CLOUD_AUTH_URL": "https://preprod-cloudv2.us.auth0.com",
            "RPK_CLOUD_AUTH_AUDIENCE": "cloudv2-preprod.redpanda.cloud",
            "CLOUD_URL": "https://cloud-api.ppd.cloud.redpanda.com/api/v1",
        }
        if self.provider == 'aws':
            self.env.update({
                "AWS_ACCESS_KEY_ID": infra_id,
                "AWS_SECRET_ACCESS_KEY": infra_secret
            })
        elif self.provider == 'gcp':
            self.env.update({"GOOGLE_APPLICATION_CREDENTIALS": infra_id})

    def _parse_plugin_list(self, plist):
        """
        Parses 'rpk plugin list' output
        """
        # 'NAME  PATH                                  SHADOWS\nbyoc  /home/ubuntu/.local/bin/.rpk.ac-byoc  \n'
        _lines = plist.split('\n')
        _headers = []
        _plugins = []
        for _line in _lines:
            # cleanup repeated spaces
            _l = ' '.join(_line.split())
            # Get nice list
            _fields = _l.lower().split()
            if not _fields:
                continue
            elif _fields[0] == 'name':
                _headers = _l.lower().split()
            elif not _headers:
                self.logger.warning(f"Error parsing rpk plugin list: {plist}")
            else:
                # Create dict
                _p = {}
                for idx in range(len(_fields)):
                    _p[_headers[idx]] = _fields[idx]
                _plugins.append(_p)
        return _plugins

    def _get_rpk_cloud_cmd(self):
        return [self.rpk._rpk_binary(), "cloud"]

    def _exec(self, cmd, timeout=60):
        self.logger.debug(f"Running '{cmd}'")
        return self.rpk._execute(cmd, env=self.env, timeout=timeout)

    def rpk_cloud_login(self, client_id, client_secret):
        # perform cloud login
        self.logger.info("Loggin in to cloud cluster")
        cmd = self._get_rpk_cloud_cmd()
        cmd += [
            "login", "--save", f"--client-id={client_id}",
            f"--client-secret={client_secret}", "--no-profile"
        ]
        out = self._exec(cmd)
        # TODO: Handle errors
        return out

    def rpk_cloud_byoc_install(self, cluster_id):
        # Install proper cloud plugin version
        self.logger.info("Installing byoc plugin according to cluster specs")
        cmd = self._get_rpk_cloud_cmd()
        cmd += ["byoc", "install", f"--redpanda-id={cluster_id}"]
        out = self._exec(cmd)
        # TODO: Handle errors
        return out

    def rpk_cloud_apply(self, cluster_id):
        self.logger.info("Deploying cluster agent")
        cmd = self._get_rpk_cloud_cmd()
        cmd += ["byoc", self.provider, "apply", f"--redpanda-id={cluster_id}"]
        if self.provider == 'gcp':
            # TODO: Research a way to get project-id from key file
            cmd += ["--project-id=devprod-cicd-infra"]
        out = self._exec(cmd, timeout=1800)
        # TODO: Handle errors
        return out

    def rpk_plugin_uninstall(self, plugin_name, sudo=False):
        self.logger.info(f"Uninstalling plugin {plugin_name}")
        _plist = self.rpk.plugin_list()
        # parse plugin list
        _plist = self._parse_plugin_list(_plist)
        _installed = [p['name'] == plugin_name for p in _plist]
        if any(_installed):
            # uninstall if plugin present. Use sudo just in case
            cmd = [] if not sudo else ["sudo"]
            cmd += [self.rpk._rpk_binary(), 'plugin', 'uninstall', plugin_name]
            out = self._exec(cmd)
            if out.startswith("unable to remove"):
                return False
            else:
                return True
        self.logger.warning(f"No plugins with the name  '{plugin_name}' found")
        return False

    def rpk_cloud_agent_delete(self, cluster_id):
        self.logger.info("Destroying cluster agent")
        cmd = self._get_rpk_cloud_cmd()
        cmd += [
            "byoc", self.provider, "destroy", f"--redpanda-id={cluster_id}"
        ]
        out = self._exec(cmd, timeout=1800)
        # TODO: Handle errors
        return out
