import asyncio
import hashlib
import logging
import re
import ssl
import time
from typing import Callable, Awaitable

import aiomqtt
from aiomqtt import Message
from meshtastic.protobuf import portnums_pb2

from src import config as cfg
from src import __version__

logger = logging.getLogger("multiplextashtic.mqtt")

MQTT_TOPIC_RE = re.compile(r"^(msh|#|\$share)/.*$")
MAX_PAYLOAD = 512


def _sanitize_client_id(raw: str) -> str:
    cleaned = re.sub(r"[^\x21-\x7E]", "", raw)
    return cleaned[:23] or "multiplextashtic"


def _node_id_to_client_suffix(node_num: int) -> str:
    return f"{node_num:08x}"


class MqttBridge:
    def __init__(self, app_config: cfg.AppConfig, node_num: int = 0):
        self._config = app_config.mqtt_bridge
        self._node_num = node_num
        self._client: aiomqtt.Client | None = None
        self._task: asyncio.Task | None = None
        self._stats_task: asyncio.Task | None = None
        self._running = False
        self._connected = asyncio.Event()
        self._downlink_cb: Callable[[str, bytes], Awaitable[None]] | None = None
        self._published = 0
        self._dropped_uplink = 0
        self._dropped_downlink_disabled = 0
        self._loop_guard_drops = 0
        self._cache = None
        self._ok_to_mqtt_missing_logged = False
        self._recent_publishes: dict[str, float] = {}
        self._echo_suppressed = 0
        self._retained_skipped = 0
        self._recent_gateway_ids: dict[int, float] = {}
        self._recent_proxy_ids: dict[int, float] = {}
        self._observed_region: str | None = None
        self._gateway_published = 0

    def _client_id(self) -> str:
        ver = __version__
        if self._config.mqtt_username:
            suffix = self._config.mqtt_username[:14]
        else:
            suffix = _node_id_to_client_suffix(self._node_num)[:14]
        raw = f"mux-v{ver}-{suffix}"
        return _sanitize_client_id(raw)

    def _topic_for_proxy(self, topic: str) -> str:
        root = self._config.root_topic.rstrip("/")
        if topic.startswith(root):
            return topic
        return f"{root}/{topic.lstrip('/')}"

    def _is_valid_downlink_topic(self, topic: str) -> bool:
        roots = (self._config.root_topic, self._config.raw_root_topic)
        if not topic.startswith(roots):
            return False
        return bool(
            MQTT_TOPIC_RE.match(topic)
            or topic.startswith(f"{self._config.root_topic}/")
            or topic.startswith(f"{self._config.raw_root_topic}/")
        )

    def resolve_region(self) -> str:
        """Resolve the region topic segment (explicit > observed > default)."""
        if self._config.region:
            return self._config.region
        if self._observed_region:
            return self._observed_region
        return "EU_868"

    def observe_proxy_topic(self, topic: str) -> None:
        """Learn the region from node-emitted proxy topics (self-updating)."""
        parts = topic.split("/")
        if len(parts) >= 2 and parts[0] == self._config.root_topic and parts[1]:
            if parts[1] != self._observed_region:
                self._observed_region = parts[1]
                logger.info(f"MqttBridge: learned region {parts[1]} from proxy topic")

    def _channel_name(self, channel_index: int) -> str | None:
        if self._cache is None:
            return None
        for ch in self._cache.channels:
            if ch.get("index") == channel_index:
                return ch.get("settings", {}).get("name") or None
        return None

    def _channel_psk(self, channel_index: int) -> bytes | None:
        if self._cache is None:
            return None
        for ch in self._cache.channels:
            if ch.get("index") == channel_index:
                psk = ch.get("settings", {}).get("psk")
                return bytes(psk) if psk else None
        return None

    def resolve_portnum_label(self, packet) -> str:
        """Portnum routing cascade for the raw topic label (label-only).

        Order: decoded present -> real name; pki_encrypted flag -> PKI;
        cached channel PSK trial decrypt -> real name; otherwise ENCRYPTED.
        Never raises; decrypted bytes never leave this function.
        """
        try:
            if packet.HasField("decoded") and packet.decoded.portnum:
                entry = portnums_pb2._PORTNUM.values_by_number.get(packet.decoded.portnum)
                return entry.name if entry else f"UNKNOWN_{packet.decoded.portnum}"
            if getattr(packet, "pki_encrypted", False):
                return "PKI"
            if packet.HasField("encrypted") and packet.encrypted:
                from src.mqtt_crypto import decrypt_data_to_portnum
                psk = self._channel_psk(packet.channel)
                if psk:
                    portnum = decrypt_data_to_portnum(
                        bytes(packet.encrypted),
                        getattr(packet, "from", 0),
                        packet.id,
                        psk,
                    )
                    if portnum:
                        entry = portnums_pb2._PORTNUM.values_by_number.get(portnum)
                        return entry.name if entry else f"UNKNOWN_{portnum}"
            return "ENCRYPTED"
        except Exception:
            return "ENCRYPTED"

    def raw_topic_for_packet(self, packet) -> str:
        """Build raw/EU_868/!senderhex/PORTNUM (never under msh/)."""
        sender = getattr(packet, "from", 0) or 0
        label = self.resolve_portnum_label(packet)
        return f"{self._config.raw_root_topic}/{self.resolve_region()}/!{sender:08x}/{label}"

    def is_proxy_covered(self, channel_index: int) -> bool:
        """True when the proxy path owns this channel (uplink flag set)."""
        if self._cache is None:
            return False
        for ch in self._cache.channels:
            if ch.get("index") == channel_index:
                return bool(ch.get("settings", {}).get("uplink_enabled", False))
        return False

    def _note_gateway_id(self, packet_id: int) -> bool:
        """Packet-id safety dedupe: True if already published recently."""
        now = time.monotonic()
        ts = self._recent_gateway_ids.get(packet_id)
        if ts is not None and now - ts < 60.0:
            return True
        self._recent_gateway_ids[packet_id] = now
        if len(self._recent_gateway_ids) > 500:
            cutoff = now - 60.0
            for k in [k for k, v in self._recent_gateway_ids.items() if v < cutoff]:
                del self._recent_gateway_ids[k]
        return False

    def note_proxy_published(self, packet_id: int) -> None:
        self._recent_proxy_ids[packet_id] = time.monotonic()
        if len(self._recent_proxy_ids) > 500:
            cutoff = time.monotonic() - 60.0
            for k in [k for k, v in self._recent_proxy_ids.items() if v < cutoff]:
                del self._recent_proxy_ids[k]

    def _recent_id(self, store: dict, packet_id: int) -> bool:
        ts = store.get(packet_id)
        if ts is None:
            return False
        if time.monotonic() - ts > 60.0:
            del store[packet_id]
            return False
        return True

    def is_proxy_published(self, packet_id: int) -> bool:
        return self._recent_id(self._recent_proxy_ids, packet_id)

    def is_gateway_published(self, packet_id: int) -> bool:
        return self._recent_id(self._recent_gateway_ids, packet_id)

    async def publish_packet(self, packet) -> bool:
        """Gateway uplink: publish one serial packet envelope to the raw tree."""
        if not self._config.enabled or not self._config.gateway_enabled:
            return False
        if getattr(packet, "via_mqtt", False):
            logger.debug("MqttBridge: gateway skipping via_mqtt packet")
            return False
        if not self._config.raw_mirror_all and self.is_proxy_published(packet.id):
            logger.debug(f"MqttBridge: gateway skipping id={packet.id} (proxy covered)")
            return False
        if self._note_gateway_id(packet.id):
            logger.debug(f"MqttBridge: gateway skipping duplicate packet id={packet.id}")
            return False
        try:
            from meshtastic.protobuf import mqtt_pb2
            env = mqtt_pb2.ServiceEnvelope()
            env.packet.CopyFrom(packet)
            env.gateway_id = f"!{self._node_num:08x}"
            payload = env.SerializeToString()
        except Exception as e:
            logger.warning(f"MqttBridge: gateway envelope build failed: {e}")
            return False
        if len(payload) > MAX_PAYLOAD:
            logger.warning(f"MqttBridge: gateway payload {len(payload)} > {MAX_PAYLOAD}, dropping")
            return False
        topic = self.raw_topic_for_packet(packet)
        if self._client is None or not self._connected.is_set():
            logger.debug("MqttBridge: gateway not connected, dropping packet")
            return False
        try:
            await self._client.publish(topic, payload=payload, qos=0, retain=False)
            self._published += 1
            self._gateway_published += 1
            self._remember_publish(topic, payload)
            logger.info(f"MqttBridge: gateway published {topic} ({len(payload)}B)")
            return True
        except Exception as e:
            logger.error(f"MqttBridge: gateway publish failed: {e}")
            return False

    async def start(self, downlink_callback: Callable[[str, bytes], Awaitable[None]] | None = None) -> None:
        if not self._config.enabled:
            logger.info("MqttBridge: not enabled, skipping start")
            return
        self._downlink_cb = downlink_callback
        self._running = True
        self._task = asyncio.create_task(self._loop())
        self._stats_task = asyncio.create_task(self._stats_loop())
        logger.info(f"MqttBridge: starting client_id={self._client_id()} {self._config.broker}:{self._config.port}")

    async def _stats_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(300)
                if self._running:
                    s = self.stats
                    logger.info(
                        "MqttBridge stats: connected=%s published=%d gateway=%d dropped_uplink=%d "
                        "dropped_downlink_disabled=%d loop_guard_drops=%d echo_suppressed=%d "
                        "retained_skipped=%d",
                        self.connected, s["published"], s["gateway_published"], s["dropped_uplink"],
                        s["dropped_downlink_disabled"], s["loop_guard_drops"],
                        s["echo_suppressed"], s["retained_skipped"],
                    )
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        stats_task = getattr(self, "_stats_task", None)
        if stats_task:
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass
            self._stats_task = None
        if self._client:
            try:
                await self._client.__aexit__()
            except Exception as e:
                logger.warning(f"MqttBridge: disconnect error: {e}")
            self._client = None
        self._connected.clear()
        logger.info("MqttBridge: stopped")

    async def _loop(self) -> None:
        backoff = 1
        while self._running:
            try:
                kwargs: dict = dict(
                    hostname=self._config.broker,
                    port=self._config.port,
                    username=self._config.username or None,
                    password=self._config.password or None,
                    identifier=self._client_id(),
                    keepalive=self._config.keepalive,
                )
                if self._config.tls:
                    kwargs["tls_context"] = ssl.create_default_context()
                async with aiomqtt.Client(**kwargs) as client:
                    self._client = client
                    self._connected.set()
                    backoff = 1
                    logger.info(f"MqttBridge: connected as {self._client_id()}")
                    if self._config.downlink_enabled and self._downlink_cb is not None:
                        topics = self._downlink_topics()
                        for t in topics:
                            await client.subscribe(t, qos=0)
                        logger.info(f"MqttBridge: subscribed to {len(topics)} downlink topics")
                    async for msg in client.messages:
                        await self._on_message(msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"MqttBridge: connection lost ({e}); reconnecting in {backoff}s")
                self._connected.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def _downlink_topics(self) -> list[str]:
        return [
            f"{self._config.root_topic}/#",
            f"{self._config.raw_root_topic}/#",
        ]

    @staticmethod
    def _echo_key(topic: str, payload: bytes) -> str:
        return f"{topic}:{hashlib.sha256(payload).hexdigest()[:16]}"

    def _remember_publish(self, topic: str, payload: bytes) -> None:
        now = time.monotonic()
        self._recent_publishes[self._echo_key(topic, payload)] = now
        if len(self._recent_publishes) > 200:
            cutoff = now - 30.0
            for k in [k for k, ts in self._recent_publishes.items() if ts < cutoff]:
                del self._recent_publishes[k]

    def _is_recent_echo(self, topic: str, payload: bytes) -> bool:
        key = self._echo_key(topic, payload)
        ts = self._recent_publishes.get(key)
        if ts is None:
            return False
        if time.monotonic() - ts > 30.0:
            del self._recent_publishes[key]
            return False
        return True

    async def _on_message(self, msg: Message) -> None:
        try:
            topic = str(msg.topic)
            payload = bytes(msg.payload) if msg.payload else b""
            if not self._is_valid_downlink_topic(topic):
                logger.debug(f"MqttBridge: ignoring non-root downlink topic {topic}")
                return
            if not self._config.downlink_enabled:
                self._dropped_downlink_disabled += 1
                logger.debug(f"MqttBridge: downlink disabled; dropped message on {topic}")
                return
            if self._is_recent_echo(topic, payload):
                self._echo_suppressed += 1
                logger.debug(f"MqttBridge: echo suppression dropped own uplink {topic}")
                return
            if getattr(msg, "retain", False):
                self._retained_skipped += 1
                logger.info(f"MqttBridge: skipping retained downlink {topic} (stale replay guard)")
                return
            if self._node_num != 0:
                try:
                    from meshtastic.protobuf import mqtt_pb2
                    env = mqtt_pb2.ServiceEnvelope()
                    env.ParseFromString(payload)
                    if env.gateway_id and env.gateway_id == f"!{self._node_num:08x}":
                        self._loop_guard_drops += 1
                        logger.debug("MqttBridge: loop guard dropped own envelope")
                        return
                except Exception:
                    pass
            await self._downlink_cb(topic, payload)
        except Exception as e:
            logger.error(f"MqttBridge: downlink processing error: {e}")

    async def publish_proxy(self, topic: str, payload: bytes, retain: bool = False) -> None:
        if not self._config.enabled or not self._config.uplink_enabled:
            self._dropped_uplink += 1
            logger.debug("MqttBridge: uplink disabled; dropped proxy message")
            return
        if not self._config.ignore_ok_to_mqtt and not self._check_uplink_allowed():
            self._dropped_uplink += 1
            logger.debug("MqttBridge: ok_to_mqtt / channel uplink check blocked publish")
            return
        final_topic = self._topic_for_proxy(topic)
        self.observe_proxy_topic(final_topic)
        if len(payload) > MAX_PAYLOAD:
            logger.warning(f"MqttBridge: payload {len(payload)} > {MAX_PAYLOAD}, dropping")
            return
        try:
            await self._ensure_connected()
            await self._client.publish(final_topic, payload=payload, qos=0, retain=retain)
            self._published += 1
            self._remember_publish(final_topic, payload)
            logger.info(f"MqttBridge: published proxy to {final_topic} ({len(payload)}B)")
        except Exception as e:
            logger.error(f"MqttBridge: publish failed: {e}")

    async def _ensure_connected(self) -> None:
        if self._client is None or not self._connected.is_set():
            raise RuntimeError("MqttBridge not connected")

    def set_cache(self, cache) -> None:
        self._cache = cache

    def _check_uplink_allowed(self) -> bool:
        if self._config.ignore_ok_to_mqtt:
            return True
        if self._cache is None:
            return True
        pkt_channel = getattr(self, "_last_channel", None)
        if pkt_channel is not None and self._cache.channels:
            for ch in self._cache.channels:
                if ch.get("index") == pkt_channel:
                    allowed = ch.get("settings", {}).get("uplink_enabled", True)
                    if not allowed and not self._ok_to_mqtt_missing_logged:
                        logger.warning("MqttBridge: channel uplink_enabled=false; set ignore_ok_to_mqtt=true to bypass")
                        self._ok_to_mqtt_missing_logged = True
                    return allowed
        return True

    def mark_channel(self, channel_index: int) -> None:
        self._last_channel = channel_index

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    @property
    def stats(self) -> dict:
        return {
            "published": self._published,
            "gateway_published": self._gateway_published,
            "dropped_uplink": self._dropped_uplink,
            "dropped_downlink_disabled": self._dropped_downlink_disabled,
            "loop_guard_drops": self._loop_guard_drops,
            "echo_suppressed": self._echo_suppressed,
            "retained_skipped": self._retained_skipped,
        }
