import json
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda_types import KafkaClientSecurity


class FakePanda:
    """A mock for a RedpandaService-like class which we pass in to
    RpkTool which allows access to some methods on RpkTool which do
    not require access to a real redpanda instance."""
    def __init__(self, context, log):
        self._context = context
        self.logger = log

    def kafka_client_security(self):
        return KafkaClientSecurity(None, False)


class CloudClusterUtils:
    def __init__(self, context, logger, infra_id, infra_secret, provider,
                 cloud_url_origin, oauth_url_origin, oauth_audience):
        """
        Initialize CloudClusterUtils.

        :param logger: logging object
        :param cluster_config: dict object loaded from
               context.globals["cloud_cluster"]
        :param infra_id: access key id
        :param infra_secret: access key secret
        :param provider: cloud provider, e.g. AWS
        :param cloud_url_origin: just scheme and hostname
        :param oauth_url_origin: just scheme and hostname
        :param oauth_audience: audience for issued token
        """
        self.fake_panda = FakePanda(context, logger)
        # Create rpk to use several functions that is isolated
        # from actual redpanda service
        self.rpk = RpkTool(self.fake_panda)
        self.logger = logger
        self.provider = provider.lower()
        self.env = {
            'RPK_CLOUD_SKIP_VERSION_CHECK': 'True',
            'RPK_CLOUD_URL': cloud_url_origin,
            'RPK_CLOUD_AUTH_URL': oauth_url_origin,
            'RPK_CLOUD_AUTH_AUDIENCE': oauth_audience,
            'CLOUD_URL': f'{cloud_url_origin}/api/v1'
        }
        if self.provider == 'aws':
            self.env.update({
                "AWS_ACCESS_KEY_ID": infra_id,
                "AWS_SECRET_ACCESS_KEY": infra_secret
            })
        elif self.provider == 'gcp':
            self.gcp_project_id = self._get_gcp_project_id(infra_id)
            self.logger.info(f"Using GCP project '{self.gcp_project_id}'")
            self.env.update({"GOOGLE_APPLICATION_CREDENTIALS": infra_id})
        elif self.provider == 'azure':
            self.subscription_id = context.globals['azure_subscription_id']
            self.logger.debug(
                f"Using Azure subscription ID: {self.subscription_id}")

    def _get_gcp_project_id(self, keyfilepath):
        project_id = None
        try:
            with open(keyfilepath, "r") as kf:
                _gcp_keyfile = json.load(kf)
                project_id = _gcp_keyfile['project_id']
        except FileNotFoundError:
            # Just catch it and pass
            pass
        # Check if succeded
        if project_id is None:
            self.logger.warning("# WARNING: GCP keyfile not found at "
                                f"'{keyfilepath}'. Check keyfile path "
                                "in globals.json")
            # Hardcoded project as a last resort
            project_id = "devprod-cicd-infra"
        return project_id

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
        self.logger.debug(f"...[{client_id}] Loggin in to cloud cluster")
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
        self.logger.debug("Installing byoc plugin according to cluster specs")
        cmd = self._get_rpk_cloud_cmd()
        cmd += ["byoc", "install", f"--redpanda-id={cluster_id}"]
        out = self._exec(cmd)
        # TODO: Handle errors
        return out

    def rpk_cloud_apply(self, cluster_id):
        self.logger.debug("Deploying cluster agent")
        cmd = self._get_rpk_cloud_cmd()
        cmd += ["byoc", self.provider, "apply", f"--redpanda-id={cluster_id}"]
        match self.provider:
            case 'gcp':
                cmd += [f"--project-id={self.gcp_project_id}"]
            case 'azure':
                cmd += [
                    f"--subscription-id={self.subscription_id}",
                    "--identity=cli", "--credential-source=cli"
                ]
        out = self._exec(cmd, timeout=1800)
        # TODO: Handle errors
        return out

    def rpk_plugin_uninstall(self, plugin_name, sudo=False):
        self.logger.debug(f"Uninstalling plugin {plugin_name}")
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
        self.logger.debug(f"No plugins with the name  '{plugin_name}' found")
        return False

    def rpk_cloud_agent_delete(self, cluster_id):
        self.logger.debug(f"...[{cluster_id}] destroying cluster agent")
        cmd = self._get_rpk_cloud_cmd()
        cmd += [
            "byoc", self.provider, "destroy", f"--redpanda-id={cluster_id}"
        ]
        if self.provider == 'gcp':
            cmd += ["--project-id=" + self.gcp_project_id]
        out = self._exec(cmd, timeout=1800)
        # TODO: Handle errors
        return out
