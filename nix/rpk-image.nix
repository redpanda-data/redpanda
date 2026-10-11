{
  dockerTools,
  cacert,
  tzdata,
  rpkDrv,
}:

dockerTools.streamLayeredImage {
  name = "redpanda-rpk";
  tag = "nix";
  maxLayers = 120;

  contents = [
    rpkDrv
    cacert
    tzdata
  ];

  config = {
    Entrypoint = [ "${rpkDrv}/bin/rpk" ];
    Env = [
      "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
    ];
  };
}
