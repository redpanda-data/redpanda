set -ex

if [[ -n "$PKG_PATH" ]]; then
    # Automatically add the host and public key to ~/.ssh/known_hosts, so that
    # scp/ ssh doesn't have to ask for confirmation interactively.
    ../../../build/bin/vtools ssh add-known-host \
                              --host "${IP}" \
                              --retries "${RETRIES}" \
                              --timeout "${TIMEOUT}"
    scp -F ./ssh_config "${PKG_PATH}" "${SSH_USER}"@"${IP}":~
fi
