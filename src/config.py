import yaml
from pathlib import Path
from typing import Literal, Optional, List
from pydantic import BaseModel, Field, field_validator


class ReconnectConfig(BaseModel):
    enabled: bool = True
    initial_delay: int = 1
    max_delay: int = 60
    multiplier: int = 2


class PhysicalNodeConfig(BaseModel):
    connection_type: Literal["serial", "tcp"] = "serial"
    serial_port: str = "/dev/ttyUSB0"
    tcp_host: str = "192.168.1.100"
    tcp_port: int = 4403
    baud_rate: int = 115200
    tcp_idle_timeout: int = 30
    reconnect: ReconnectConfig = ReconnectConfig()


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 4404
    max_clients: int = 20


class VirtualNodeConfig(BaseModel):
    long_name: str = "Virtual Multiplexer"
    short_name: str = "VMUX"
    sync_physical_identity: bool = True


class SecurityConfig(BaseModel):
    blocked_portnums: List[str] = Field(default_factory=lambda: ["ADMIN_APP"])
    allow_admin_from_ips: List[str] = Field(default_factory=list)


class DiscoveryConfig(BaseModel):
    enabled: bool = True


class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    file: Optional[str] = "logs/multiplexer.log"
    console: bool = True


class AppConfig(BaseModel):
    physical_node: PhysicalNodeConfig = PhysicalNodeConfig()
    server: ServerConfig = ServerConfig()
    virtual_node: VirtualNodeConfig = VirtualNodeConfig()
    security: SecurityConfig = SecurityConfig()
    discovery: DiscoveryConfig = DiscoveryConfig()
    logging: LoggingConfig = LoggingConfig()

    @classmethod
    def from_yaml(cls, path: str = "config.yaml") -> "AppConfig":
        p = Path(path)
        if not p.exists():
            return cls()
        with open(p, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls(**data)

    @field_validator("server")
    @classmethod
    def validate_server_port(cls, v: ServerConfig) -> ServerConfig:
        if v.port == 4403:
            raise ValueError("Server port 4403 conflicts with physical node default. Use 4404 or higher.")
        return v
