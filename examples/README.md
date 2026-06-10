# Go Stratum TCP Proxy Configuration Scenarios

This directory contains configuration examples for different multi-backend stratum setups. These scenarios demonstrate how to configure both the VPS Server (`backends.json`) and the Local Agents (`agent.json`) for various production layouts.

---

## Scenario 1: High Availability (HA) with Failover & Cooldown

### Description
In this scenario, you have a group of miners mining the same coin (e.g., `NENG`), and you want to route all traffic to a **Primary Local Pool (Agent A)**. If Agent A goes offline (due to ISP disconnect, power outage, or maintenance), the proxy server should automatically route new miners to a **Backup Local Pool (Agent B)**.

To prevent connection cutoff or constant jumping when Agent A flaps, a **failback cooldown** of 8 hours is enabled.

### Configuration Files
- **VPS Server**: [backends_scenario1.json](file:///home/sena/Documents/script/proxy/examples/backends_scenario1.json)
- **Local Agent A (Primary)**: [agent_scenario1_primary.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario1_primary.json)
- **Local Agent B (Backup)**: [agent_scenario1_backup.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario1_backup.json)

---

## Scenario 2: Static Coin Mapping (No Failover)

### Description
In this scenario, you run multiple different coins (e.g., `BTG` and `BTB`) on separate, specialized mining pool engines. Because the BTG backend engine cannot process BTB traffic (and vice-versa), **failover must be disabled**. 

Using `"static_mapping": true`, if the BTG primary backend goes offline, BTG miners will fail immediately and will **never** be routed to the BTB backend.

### Configuration Files
- **VPS Server**: [backends_scenario2.json](file:///home/sena/Documents/script/proxy/examples/backends_scenario2.json)
- **BTG Pool Agent**: [agent_scenario2_btg.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario2_btg.json)
- **BTB Pool Agent**: [agent_scenario2_btb.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario2_btb.json)

---

## Scenario 3: Mixed / Hybrid Configuration

### Description
A hybrid production environment where:
- `group_neng` routes `NENG` and has automatic failover enabled between a Primary (Priority 1) and Backup (Priority 2) agent.
- `group_nxe` routes `NXE` but is strictly locked with `"static_mapping": true`. It only allows connections to its Primary NXE Agent; if it goes offline, miners disconnect instead of falling back to other backends.

### Configuration Files
- **VPS Server**: [backends_scenario3.json](file:///home/sena/Documents/script/proxy/examples/backends_scenario3.json)
- **Agent 1 (NENG Primary)**: [agent_scenario3_neng_primary.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario3_neng_primary.json)
- **Agent 2 (NENG Backup)**: [agent_scenario3_neng_backup.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario3_neng_backup.json)
- **Agent 3 (NXE Static)**: [agent_scenario3_nxe_static.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario3_nxe_static.json)

---

## Scenario 4: Multi-Difficulty Port Mapping

### Description
In this scenario, your local stratum pool backend uses different ports to listen for miners of different difficulties (e.g. Normal, Low, and High). You want the VPS to expose three matching dedicated ports. Connections coming into each VPS port are routed directly to the corresponding local pool port without needing custom protocol control headers.

### Configuration Files
- **VPS Server**: [backends_scenario4.json](file:///home/sena/Documents/script/proxy/examples/backends_scenario4.json)
- **Local Agent Config**: [agent_scenario4.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario4.json)

---

## Scenario 5: Large Hybrid Environment (Dynamic Routing + Multiple Dedicated Ports)

### Description
A large-scale hybrid mining layout containing multiple coins (NENG, NXE, MTBC, DOGE, LTC, BTC, BCH) where:
- A global port (`33330`) dynamically routes incoming miners to either the Scrypt (`group_neng_dynamic` supporting NENG/MTBC) or SHA256 (`group_sha256_dynamic` supporting BTC/BCH) backend pools based on payload parsing.
- Dedicated VPS ports route traffic directly to specific coins or difficulty levels without checking payload content:
  - Port `33331` maps directly to Scrypt Low-Difficulty NENG.
  - Port `33332` maps directly to Scrypt High-Difficulty NENG.
  - Port `33333` maps directly to dedicated NXE mining.
  - Port `33334` maps directly to dedicated DOGE mining.
  - Port `33335` maps directly to dedicated LTC mining.

### Configuration Files
- **VPS Server**: [backends_scenario5.json](file:///home/sena/Documents/script/proxy/examples/backends_scenario5.json)
- **Local Scrypt Agent**: [agent_scenario5_scrypt.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario5_scrypt.json)
- **Local SHA256 Agent**: [agent_scenario5_sha256.json](file:///home/sena/Documents/script/proxy/examples/agent_scenario5_sha256.json)
