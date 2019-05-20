#pragma once
inline uint16_t non_root_port(uint16_t port) {
    if (port < 1024)
        return port + 1024;
    return port;
}
