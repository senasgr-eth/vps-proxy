# Go Stratum TCP Proxy Configuration Examples

This directory contains configuration examples for the Go Stratum TCP Proxy in strictly static mapping mode.

---

## Directory Contents

- **VPS Server Configuration**: [backends.json](file:///home/sena/Documents/script/proxy/examples/backends.json)
- **Local Agent Configuration**: [agent.json](file:///home/sena/Documents/script/proxy/examples/agent.json)

---

## Static Mapping Layout

In this configuration:
1. The VPS server listens on port `44444` for incoming Tunnel Agents.
2. The VPS server exposes:
   - A global miner port `33333` which scans the first packet for coin symbols (`c=NENG`, `c=LTC`, `c=NENG_LOW`) and routes statically to `group_scrypt` or `group_neng_lowdiff`.
   - Dedicated miner ports per group (`30100` and `30101`) which route directly to the mapped backend without payload scanning.
3. The local Tunnel Agent dials outbound to the VPS server on port `44444` using the shared `secret_token` and TLS encryption.
4. The local Agent maintains idle connection pools matching the server's groups and tunnels them to local backend pools:
   - Traffic for `group_scrypt` is routed to the local scrypt pool listening on `127.0.0.1:32221`.
   - Traffic for `group_neng_lowdiff` is routed to the local low-diff pool listening on `127.0.0.1:32222`.
5. If a group has no tunnels online, the connection is dropped immediately (no failover or default fallback).
