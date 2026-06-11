# Go Stratum TCP Tunnel Proxy

A high-performance, zero-dependency, reverse-tunneling Stratum TCP Proxy written in Go. 

This architecture allows a local mining pool backend situated behind a NAT or firewall to connect outbound to a public VPS (static IP). The VPS acts as the **Tunnel Server** (routing miner connections) and the local pool runs the **Tunnel Agent** (maintaining the pools of tunnel streams).

### Key Architectural Benefits
1. **Bypasses NAT/Firewalls**: The Agent dials outbound to the VPS. No ports need to be forwarded on your local network.
2. **No Dynamic IP Updating Needed**: Since the Agent initiates the connection, the VPS does not need to know the backend's IP. If your local ISP reconnects and changes your dynamic IP, the agent simply reconnects to the VPS automatically.
3. **Flexible Static Routing Options**:
   - **Global Port (Coin Scanning)**: Expose a single port (e.g. `33333`) on the VPS. The proxy scans incoming payloads on this port for coin symbols (like `c=NENG` or `c=NXE`) and routes statically to the matched group. If unrecognized or offline, it drops the connection immediately (no failover/fallback).
   - **Dedicated Port Mapping (Bypass Scanning)**: Open dedicated listen ports per group (e.g. `30100`). Connections on these ports map directly to their corresponding tunnel groups, ensuring zero packet-splitting delays or dynamic payload-scanning.
4. **FIFO Connection Allocation**: To ensure stratum stream stability, idle connections are popped in First-In, First-Out (FIFO) order.

---

## Architectural Flow

```mermaid
graph TD
    MinerGlobal1[Miner NENG] -->|Connects to VPS:33333| Server[Go Stratum Proxy Server on VPS]
    MinerGlobal2[Miner NXE] -->|Connects to VPS:33333| Server
    MinerScrypt[Miner Dedicated Scrypt] -->|Connects to VPS:30100| Server
    
    subgraph local_network ["Local Backend Network (Behind NAT)"]
        AgentA[Tunnel Agent - Group Scrypt] -->|Outbound dials VPS:44444| Server
        AgentA -->|Dials local pool| PoolA[Local Pool Scrypt :32221]
        
        AgentB[Tunnel Agent - Group NXE] -->|Outbound dials VPS:44444| Server
        AgentB -->|Dials local pool| PoolB[Local Pool NXE :32222]
    end

    subgraph vps_routing ["VPS Server Routing"]
        Server -->|Scans c=NENG or maps VPS:30100| TunnelPoolA[Group Scrypt Tunnel Pool]
        TunnelPoolA -->|Pipes stream| MinerGlobal1

        Server -->|Scans c=NXE| TunnelPoolB[Group NXE Tunnel Pool]
        TunnelPoolB -->|Pipes stream| MinerGlobal2
    end
```

---

## Configuration

### 1. Server Configuration (`/etc/stratum-proxy/backends.json`)
Placed on the VPS. It configures the ports, security settings, and static routing group configurations:

```json
{
  "listen": "0.0.0.0:33333", // Optional global port
  "tunnel_listen": "0.0.0.0:44444",
  "secret_token": "a_very_long_secure_shared_token_string",
  "tls_cert": "/etc/stratum-proxy/cert.pem",
  "tls_key": "/etc/stratum-proxy/key.pem",
  "max_connections": 10000,
  "groups": [
    {
      "name": "group_scrypt",
      "listen": "0.0.0.0:30100", // Optional dedicated port
      "coins": ["NENG", "LTC", "MTBC"] // Coin symbols for global port matching
    },
    {
      "name": "group_nxe",
      "listen": "0.0.0.0:30101",
      "coins": ["NXE", "BTG"]
    }
  ]
}
```

### 2. Agent Configuration (`/etc/stratum-agent/agent.json`)
Placed on the local mining pool machine. It establishes tunnel pools matching coin groups, mapping them to the local backend port:

```json
{
  "server": "vps_public_ip:44444",
  "pool_size": 5,
  "secret_token": "a_very_long_secure_shared_token_string",
  "tls": true,
  "tls_skip_verify": true,
  "mappings": [
    {
      "group": "group_scrypt",
      "local": "127.0.0.1:32221"
    },
    {
      "group": "group_nxe",
      "local": "127.0.0.1:32222"
    }
  ]
}
```

---

## Security & Encryption (Optional TLS & Token Auth)

To protect your tunnel connections from unauthorized agents and eavesdropping, the proxy features a secure authentication and encryption layer:

1. **Pre-Shared Token Authentication**: Only authorized Agents that present the configured `secret_token` can register tunnels with the VPS. The server immediately closes connections with missing or invalid tokens.
2. **Dynamic TLS Detection (Optional)**: TLS is optional.
   - **On the Agent**: If `"tls": true` is specified in `agent.json`, the agent initiates a TLS connection to wrap all tunnel traffic. If `"tls": false` (or omitted), it connects using raw TCP (passing raw data).
   - **On the VPS Server**: If `tls_cert` and `tls_key` are specified in `backends.json`, the server supports TLS dynamically on the *same* tunnel port. It inspects the first byte of incoming connections. If a TLS handshake is detected (first byte is `0x16`), it negotiates TLS; if not, it falls back to raw TCP.

### Generating a Self-Signed TLS Certificate

For ease of deployment, you can generate a self-signed certificate directly on the VPS to use for the tunnel encryption:

```bash
# Generate a self-signed certificate and private key valid for 10 years (3650 days)
sudo openssl req -x509 -newkey rsa:4096 -nodes -keyout /etc/stratum-proxy/key.pem -out /etc/stratum-proxy/cert.pem -sha256 -days 3650 -subj "/CN=stratum-proxy"
```

Once generated, make sure to point the `tls_cert` and `tls_key` fields in `backends.json` to these paths, and set `"tls": true` in `agent.json`.

---

## Compilation & Verification

### Run Automated Tests
Verifies proxy routing, dynamic agent reconnects, FIFO selection, authentication, connection limits, and panic recovery:

```bash
/usr/local/go/bin/go test -v ./...
```

### Building Binaries
For simplicity, you can use the helper script `./build.sh` to compile your binaries:

1. **Build for your current system**:
   ```bash
   ./build.sh
   # Binaries will be built in: build/bin/
   ```

2. **Cross-compile for all targets (Linux AMD64 and ARM64)**:
   ```bash
   ./build.sh all
   # Binaries will be built in: build/linux-amd64/ and build/linux-arm64/
   ```

3. **Clean up build artifacts**:
   ```bash
   ./build.sh clean
   ```

---

## How Static Mapping & FIFO Works

1. **FIFO Pool Mapping**: The Agent maintains a constant pool of `pool_size` idle connections to the VPS. Inside the VPS, these connections are sorted by registration time (FIFO). When a miner connects, the VPS selects the oldest idle connection in the pool. This minimizes connection cycling.
2. **Zero Failover / Cooldown**: There are no failover mechanisms, secondary fallback agents, or cooldown periods. If a miner matches a group (either via a dedicated port or by scanning `c=coin` on the global port), it is mapped statically. If that group has no active tunnel connections from an agent, the connection is dropped immediately.
3. **Verbose Log Troubleshooting**: If a miner fails to route on the global port, the proxy prints a verbose log showing the full incoming Stratum payload string (up to 1024 bytes) to help troubleshoot missing or misplaced coin symbols.
