"""Firmware-compatible Meshtastic channel crypto (routing-label use only).

Ported from meshtastic/firmware (master, MIT):
- src/mesh/CryptoEngine.cpp: encryptPacket/decrypt/initNonce/encryptAESCtr
- src/mesh/CryptoEngine.h: nonce layout
    (64-bit packet number LE + 32-bit sender LE + 32-bit block counter @ 0)
- src/mesh/Channels.cpp: Channels::getKey PSK expansion rules

Decrypted bytes are used ONLY to derive the MQTT topic label for gateway
uplink routing. They must never be published, logged, or stored.

CTR counter byte order follows rweather/Arduino-Crypto CTR with
setCounterSize(4): the trailing 4 nonce bytes increment big-endian.
Proven against real firmware via the downlink oracle test
(Python-encrypted envelope accepted by the node); see tests.
"""

import logging
import struct

from Crypto.Cipher import AES

from meshtastic.protobuf import mesh_pb2

logger = logging.getLogger("multiplextashtic.crypto")

MAX_BLOCKSIZE = 256  # firmware CryptoEngine.h


def expand_psk(psk: bytes) -> bytes:
    """Expand a channel PSK into a usable AES key (Channels::getKey rules).

    Returns 16 (AES128) or 32 (AES256) bytes. Raises ValueError when the
    channel is unencrypted (empty) or uses the 1-byte default-key shorthand
    (firmware `defaultpsk` bytes not vendored here).
    """
    if len(psk) == 0:
        raise ValueError("empty PSK: channel is unencrypted")
    if len(psk) == 1:
        if psk[0] == 0:
            raise ValueError("PSK index 0: encryption disabled")
        raise ValueError(
            "1-byte default-key shorthand (index %d) is not supported" % psk[0]
        )
    if len(psk) < 16:
        # Firmware pads short keys with zeros (key buffer is memset first).
        return (psk + b"\x00" * 16)[:16]
    if len(psk) == 16:
        return psk
    if len(psk) < 32:
        return (psk + b"\x00" * 32)[:32]
    return psk[:32]


def build_nonce(from_node: int, packet_id: int) -> bytes:
    """Build the 16-byte CTR nonce (CryptoEngine::initNonce, no extraNonce)."""
    return struct.pack("<Q", packet_id & 0xFFFFFFFFFFFFFFFF) + struct.pack(
        "<I", from_node & 0xFFFFFFFF
    ) + b"\x00\x00\x00\x00"


def aes_ctr_crypt(key: bytes, nonce16: bytes, data: bytes) -> bytes:
    """AES-CTR crypt (firmware encryptAESCtr, counter size 4, big-endian).

    The trailing 4 nonce bytes are the starting block counter (firmware
    always starts at zero; RFC 3686 vectors start at one).
    """
    if len(key) not in (16, 32):
        raise ValueError(f"key must be 16 or 32 bytes, got {len(key)}")
    if len(nonce16) != 16:
        raise ValueError("nonce must be 16 bytes")
    cipher = AES.new(
        key,
        AES.MODE_CTR,
        nonce=nonce16[:12],
        initial_value=int.from_bytes(nonce16[12:], "big"),
    )
    return cipher.encrypt(data)


def crypt_data(psk: bytes, from_node: int, packet_id: int, data: bytes) -> bytes:
    """Encrypt/decrypt a Data payload (CTR is symmetric)."""
    if len(data) > MAX_BLOCKSIZE:
        raise ValueError(f"payload {len(data)}B exceeds {MAX_BLOCKSIZE}B")
    key = expand_psk(psk)
    nonce = build_nonce(from_node, packet_id)
    return aes_ctr_crypt(key, nonce, data)


def decrypt_data_to_portnum(
    ciphertext: bytes, from_node: int, packet_id: int, psk: bytes
) -> int | None:
    """Trial-decrypt channel ciphertext, return Data.portnum or None.

    Returns None (never raises) when the key is wrong, the nonce mismatches,
    or the plaintext does not parse as a Data protobuf.
    """
    try:
        plaintext = crypt_data(psk, from_node, packet_id, ciphertext)
    except ValueError:
        return None
    try:
        data = mesh_pb2.Data()
        data.ParseFromString(plaintext)
        if not data.portnum:
            return None
        return data.portnum
    except Exception:
        return None
