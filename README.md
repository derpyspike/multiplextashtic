# Multiplextashtic

A Python asyncio multiplexer that allows **multiple Meshtastic-compatible mobile/desktop apps** to connect simultaneously to a **single physical node** running Meshtastic firmware.

> **⚠️ This project has been entirely vibecoded.** Contributions are welcome — feel free to open a pull request with any improvement.

```
App (Client 1) ──┐
App (Client 2) ──┤── TCP:4404 ──► Multiplexer ── serial/TCP:4403 ──► Physical Node
App (Client N) ──┘                (Python asyncio)
```

## Credits

This project is **an inspiration** of [Yeraze's MeshMonitor Virtual Node Server](https://github.com/Yeraze/meshmonitor).

Key differences from Yeraze:
- **No database layer** — everything runs in-memory
- **Configurable IP allowlist** for admin commands instead of a binary flag
- **Raw byte transport** (matches Yeraze's TCP transport, no pubsub library)
- **mDNS service discovery** added for LAN auto-discovery

## Features

- **Multi-client support** — unlimited simultaneous TCP clients on port 4404
- **Config replay** — auto-captures physical node state and replays it to new clients (MyNodeInfo, channels, configs, ~200 synthetic NodeInfos)
- **Raw byte transport** — connects via raw serial or TCP, reads framed `FromRadio` bytes directly (no pubsub library), matching Yeraze's architecture
- **mDNS service discovery** — announces on the LAN so apps find it automatically
- **Security filtering** — blocks ADMIN_APP from client→physical direction; IP allowlist for admin commands
- **Admin command interception** — `removeByNodenum` gets a fake ACK (prevents UI hang), `addContact` blocked from all IPs
- **Self-addressed admin bypass** — `from==to` queries (safe read-only ops) bypass the IP whitelist
- **PKI encryption stripping** — strips PKI encryption from `from=0` packets before forwarding
- **Heartbeat → QueueStatus** — responds to client heartbeats to keep iOS/Android apps alive
- **Client lifecycle management** — inactivity timeout (5 min), cleanup loop (1 min), config request rate limiting (5 s)
- **Bounded message queue** — max 100 messages with 10 ms send delay
- **TCP idle timeout** — detects silent disconnects (no data for 30s), triggers exponential backoff reconnection  
- **Config re-capture on reconnect** — re-captures config and refreshes all connected clients
- **Mock mode** — run without real hardware for development/testing
- **Docker support** — multi-stage build, non-root user, health check, `.dockerignore`

## Requirements

- Python 3.10+
- A node running Meshtastic firmware connected via USB (serial) or TCP
- `meshtastic`
- `protobuf`
- `pyyaml>=6.0`
- `pydantic>=2.0`
- `zeroconf>=0.132.0` (for mDNS service discovery)

## Installation

```bash
git clone <repo-url>
cd multiplextashtic
pip install -r requirements.txt
```

## Configuration

Copy the example config and customize:

```bash
cp configs/config.example.yaml configs/config.yaml
```

### Key configuration sections

```yaml
physical_node:
  connection_type: "serial"        # "serial" or "tcp"
  serial_port: "COM55"             # Windows
  # serial_port: "/dev/ttyUSB0"    # Linux
  baud_rate: 115200

    # TCP settings (used when connection_type = "tcp")
  tcp_host: "192.168.1.100"
  tcp_port: 4403
  tcp_idle_timeout: 30           # seconds without data before assuming TCP disconnect
  reconnect:
    enabled: true
    initial_delay: 1               # seconds
    max_delay: 60
    multiplier: 2                  # exponential backoff

server:
  host: "0.0.0.0"
  port: 4404                       # multiplexer listen port
  max_clients: 20

virtual_node:
  long_name: "Virtual Multiplexer"
  short_name: "VMUX"
  sync_physical_identity: true

security:
  blocked_portnums:
    - "ADMIN_APP"
  allow_admin_from_ips: []         # empty = block all remote admin

discovery:
  enabled: true

logging:
  level: "INFO"                    # DEBUG, INFO, WARNING, ERROR
  file: "logs/multiplexer.log"
  console: true
```

## Usage

### Quick start (mock mode — no hardware needed)

```bash
python -m src.main --mock
```

### With a real ESP32 connected via serial

```bash
python -m src.main --config configs/config.yaml
```

### With a real ESP32 connected via TCP

Set `connection_type: "tcp"`, `tcp_host`, and `tcp_port` in `configs/config.yaml`, then:

```bash
python -m src.main --config configs/config.yaml
```

### Command-line arguments

| Argument | Description |
|----------|-------------|
| `-c, --config PATH` | Path to YAML config (default: `configs/config.yaml`) |
| `--mock` | Use simulated physical node (no real hardware) |

## Connecting clients

Once the multiplexer is running, point any compatible app to `IP:4404`:

- **Android app:** Settings → Radio Configuration → Network → "TCP Client" → enter IP and port 4404
- **iOS app:** Settings → Connect → TCP → enter IP and port 4404
- **meshtastic CLI:** `meshtastic --host IP --port 4404 --info`
- **meshtastic CLI (mock):** `meshtastic --host 127.0.0.1 --port 4404 --info`

### mDNS auto-discovery

If `discovery.enabled: true` (default), the multiplexer announces itself as `Multiplextashtic._meshtastic._tcp.local.` on the LAN. Compatible mobile apps should discover it automatically without manual IP entry.

> **Note on Windows:** The built-in Windows mDNS responder holds port 5353, which can prevent the Python zeroconf library from responding to mDNS queries. The service registers (visible in logs) but may not appear in network scans. On Linux/Docker this works reliably.

## Docker

```bash
# Build and run
docker compose up -d

# Or build manually
docker build -t multiplextashtic .
docker run -d --name multiplextashtic -p 4404:4404 \
  -v ./configs/config.yaml:/app/configs/config.yaml:ro \
  -v ./logs:/app/logs \
  multiplextashtic
```

> **mDNS on Docker:** For mDNS discovery to work in Docker, you need `network_mode: host` in docker-compose so the container can send multicast packets. Uncomment the relevant section in `docker-compose.yml` if you need mDNS.

The container runs as a non-root `mux` user with a health check on port 4404.

## Project structure

```
├── src/
│   ├── main.py                 # Entry point, logging, wiring, callbacks
│   ├── config.py               # Pydantic config with YAML loading
│   ├── protocol.py             # Frame format: 0x94 0xC3 + 2-byte BE length
│   ├── physical_node.py        # Serial/TCP connection to real node with reconnect
│   ├── mock_physical_node.py   # Simulated node for dev/testing
│   ├── tcp_server.py           # Async TCP server listening on port 4404
│   ├── client_handler.py       # Per-client lifecycle (read/write loops, config replay)
│   ├── message_router.py       # Bidirectional routing with security filtering
│   ├── config_capture.py       # Reads node config state, builds init sequence
│   ├── mdns_service.py         # mDNS service discovery (_meshtastic._tcp)
│   ├── message_queue.py        # Bounded async queue (max 100, 10ms delay)
│   └── state_cache.py          # In-memory cache of node state
├── configs/
│   ├── config.yaml             # Your configuration (not in repo)
│   └── config.example.yaml     # Reference configuration
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run with mock mode E2E
python scripts/test_mock_e2e.py
python scripts/test_end_to_end.py
```

## Security model

| Direction | Portnums blocked | Notes |
|-----------|-----------------|-------|
| Client → Physical | ADMIN_APP | Blocks unsafe admin commands from non-whitelisted IPs |
| Physical → Client | None | All packets forwarded (only `config_complete_id` filtered to match Yeraze) |

**Admin command exceptions:**
- Localhost (`127.0.0.1`, `::1`) — always allowed
- IP/CIDR allowlist (`security.allow_admin_from_ips`) — supports individual IPs and CIDR subnets (e.g. `"192.168.1.0/24"` for whole LAN)
- Self-addressed queries (`from==to`) — always allowed (safe read-only ops like `--get-config`)
- `removeByNodenum` — intercepted, fake ACK sent back, command dropped
- `addContact` — blocked from ALL IPs including localhost

## Init sequence (PhoneAPI order)

When a client connects, the multiplexer replays the physical node's state. The sequence depends on the `want_config_id` nonce:

- **Full (random nonce):** MyNodeInfo → OwnNodeInfo → Metadata → Channels → Configs → ModuleConfigs → NodeInfos → ConfigComplete
- **69420 (config-only):** Same without NodeInfos (client only needs config refresh)
- **69421 (DB-only):** OwnNodeInfo + NodeInfos + ConfigComplete (client only needs node list refresh)

Message order matches the PhoneAPI.cpp state machine: MyNodeInfo, OwnNodeInfo, Metadata, Channels, Configs, ModuleConfigs, NodeInfos, ConfigComplete.

## Protocol

The wire format matches the Meshtastic-compatible PhoneAPI TCP framing:

- **Start bytes:** `0x94 0xC3`
- **Length:** 2-byte big-endian (max payload: 512 bytes)
- **Payload:** Serialized protobuf
- **Client → Server:** `ToRadio` protobuf
- **Server → Client:** `FromRadio` protobuf

## Known limitations

- **mDNS on Windows:** Python zeroconf competes with the built-in Windows mDNS responder for port 5353. Works reliably on Linux/Docker.
- **No database layer:** Node state is held in memory (lost on restart). Config is re-captured from device defaults on reconnect.
- **No MQTT proxy support:** MQTT proxy messages from clients are silently dropped. [WIP]

## License

MIT
