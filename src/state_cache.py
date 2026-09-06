import copy
import logging

logger = logging.getLogger("multiplextashtic.cache")


class StateCache:
    def __init__(self):
        self.my_node_num: int = 0
        self.my_info: dict | None = None
        self.node_info: dict | None = None
        self.metadata: dict | None = None
        self.channels: list[dict] = []
        self.configs: list[dict] = []
        self.module_configs: list[dict] = []
        self.known_nodes: dict[int, dict] = {}

    def update_my_info(self, my_info: dict) -> None:
        self.my_info = my_info
        self.my_node_num = my_info.get("my_node_num", 0)
        logger.debug(f"Cache: MyInfo updated (node_num=0x{self.my_node_num:08x})")

    def update_node_info(self, node_info: dict) -> None:
        self.node_info = node_info
        num = node_info.get("num", 0)
        uid = node_info.get("user", {}).get("id", "?")
        logger.debug(f"Cache: NodeInfo updated (num=0x{num:08x}, id={uid})")

    def update_channel(self, channel: dict) -> None:
        index = channel.get("index", len(self.channels))
        existing = next((i for i, c in enumerate(self.channels) if c.get("index") == index), None)
        if existing is not None:
            self.channels[existing] = channel
        else:
            self.channels.append(channel)
        self.channels.sort(key=lambda c: c.get("index", 0))
        logger.debug(f"Cache: Channel {index} updated")

    def update_config(self, config: dict) -> None:
        self.configs.append(config)
        logger.debug(f"Cache: Config added ({len(self.configs)} total)")

    def update_module_config(self, config: dict) -> None:
        self.module_configs.append(config)
        logger.debug(f"Cache: ModuleConfig added ({len(self.module_configs)} total)")

    def update_known_node(self, node_info: dict) -> None:
        num = node_info.get("num", 0)
        self.known_nodes[num] = node_info
        logger.debug(f"Cache: Known node 0x{num:08x} stored ({len(self.known_nodes)} total)")

    def get_primary_channel_psk(self) -> bytes | None:
        for ch in self.channels:
            if ch.get("role") == 1:
                return ch.get("settings", {}).get("psk")
        return None

    def get_initial_config_data(self) -> dict:
        return {
            "my_info": copy.deepcopy(self.my_info),
            "node_info": copy.deepcopy(self.node_info),
            "metadata": copy.deepcopy(self.metadata),
            "channels": copy.deepcopy(self.channels),
            "configs": copy.deepcopy(self.configs),
            "module_configs": copy.deepcopy(self.module_configs),
            "known_nodes": copy.deepcopy(self.known_nodes),
        }
