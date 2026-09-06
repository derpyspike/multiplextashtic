"""Bounded async message queue for client-to-physical-node messages.

Matches Yeraze's VirtualNodeServer queueMessage + processQueue pattern:
- Max 100 queued messages (drops if full)
- 10ms delay between sends to avoid overwhelming the physical node
- Single processor task drains the queue sequentially
"""

import asyncio
import logging

logger = logging.getLogger("multiplextashtic.queue")

QUEUE_MAX_SIZE = 100
SEND_DELAY_SECONDS = 0.01


class MessageQueue:
    def __init__(self, phys_mgr):
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
        self._phys_mgr = phys_mgr
        self._processing = False

    async def enqueue_raw(self, raw_to_radio: bytes) -> bool:
        try:
            self._queue.put_nowait(raw_to_radio)
            logger.debug(f"MessageQueue: enqueued {len(raw_to_radio)} bytes (size={self._queue.qsize()})")
            if not self._processing:
                asyncio.create_task(self._process())
            return True
        except asyncio.QueueFull:
            logger.warning(f"MessageQueue: queue full ({QUEUE_MAX_SIZE}), dropping message")
            return False

    async def _process(self):
        if self._processing:
            return
        self._processing = True
        try:
            while True:
                try:
                    raw = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                try:
                    await self._phys_mgr.send_raw_to_radio(raw)
                except Exception as e:
                    logger.error(f"MessageQueue: failed to send: {e}")

                await asyncio.sleep(SEND_DELAY_SECONDS)
        except asyncio.CancelledError:
            raise
        finally:
            self._processing = False
            if self._queue.qsize() > 0:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop is not None:
                    asyncio.create_task(self._process())
