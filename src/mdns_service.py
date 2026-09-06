import asyncio
import logging
import socket

from zeroconf import IPVersion, ServiceInfo, Zeroconf

from src import __version__
from src import config as cfg

logger = logging.getLogger("multiplextashtic.mdns")

_MDNS_TIMEOUT = 5.0


def _get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


class MDNSService:
    def __init__(self, app_config: cfg.AppConfig, short_name: str = "VMUX", node_id: str = "!00000000"):
        self._config = app_config.discovery
        self._server_config = app_config.server
        self._short_name = short_name
        self._node_id = node_id
        self._zc: Zeroconf | None = None
        self._info: ServiceInfo | None = None

    def set_short_name(self, name: str) -> None:
        self._short_name = name

    async def start(self) -> None:
        if not self._config.enabled:
            logger.info("mDNS discovery disabled")
            return

        try:
            local_ip = _get_local_ip()
            service_type = "_meshtastic._tcp.local."
            service_name = "Multiplextashtic"

            properties = {
                b"shortname": self._short_name.encode("utf-8"),
                b"id": self._node_id.encode("utf-8"),
                b"version": __version__.encode("utf-8"),
                b"pio_env": b"python-mux",
            }

            self._info = ServiceInfo(
                type_=service_type,
                name=f"{service_name}.{service_type}",
                addresses=[socket.inet_aton(local_ip)],
                port=self._server_config.port,
                properties=properties,
                server=f"{service_name}.local.",
            )

            def _register():
                zc = Zeroconf(ip_version=IPVersion.V4Only)
                zc.register_service(self._info)
                return zc

            self._zc = await asyncio.wait_for(
                asyncio.to_thread(_register),
                timeout=_MDNS_TIMEOUT,
            )

            logger.info(
                f"mDNS service registered: Multiplextashtic "
                f"(_meshtastic._tcp.local.) port={self._server_config.port} "
                f"shortname={self._short_name} id={self._node_id}"
            )
        except asyncio.TimeoutError:
            logger.warning("mDNS registration timed out (port 5353 may be in use)")
        except Exception as e:
            logger.warning(f"Failed to register mDNS service: {e}")

    async def stop(self) -> None:
        if self._zc:
            try:
                if self._info:
                    await asyncio.to_thread(self._zc.unregister_service, self._info)
            except Exception:
                pass
            try:
                await asyncio.to_thread(self._zc.close)
            except Exception:
                pass
            self._zc = None
            self._info = None
            logger.info("mDNS service unregistered")
