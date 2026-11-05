"""Device records and address uniqueness within one registry instance."""
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, IPvAnyAddress


class DeviceKind(str, Enum):
    SWITCH = "switch"
    ROUTER = "router"
    ACCESS_POINT = "access_point"


class DeviceState(str, Enum):
    ACTIVE = "active"
    MAINTENANCE = "maintenance"
    DISABLED = "disabled"


class DeviceInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    name: str = Field(min_length=1, max_length=120)
    ip_address: IPvAnyAddress
    kind: DeviceKind
    state: DeviceState = DeviceState.ACTIVE


class DeviceRecord(DeviceInput):
    model_config = ConfigDict(frozen=True)
    id: str


class AddressConflict(Exception):
    pass


class DeviceNotFound(Exception):
    pass


class DeviceRegistry:
    """RAM-only registry with a normalized IP-to-device index."""

    def __init__(self):
        self._devices: dict[str, DeviceRecord] = {}
        self._addresses: dict[str, str] = {}

    def create(self, device: DeviceInput) -> DeviceRecord:
        address = str(device.ip_address)
        if address in self._addresses:
            raise AddressConflict("IP address is already registered")
        record = DeviceRecord(id=str(uuid4()), **device.model_dump())
        self._devices[record.id] = record
        self._addresses[address] = record.id
        return record

    def get(self, device_id: str) -> DeviceRecord:
        if device_id not in self._devices:
            raise DeviceNotFound("Device not found")
        return self._devices[device_id]

    def replace(self, device_id: str, device: DeviceInput) -> DeviceRecord:
        previous = self.get(device_id)
        address = str(device.ip_address)
        owner = self._addresses.get(address)
        if owner is not None and owner != device_id:
            raise AddressConflict("IP address belongs to another device")
        replacement = DeviceRecord(id=device_id, **device.model_dump())
        # Validate first: a rejected update must preserve the old record and index.
        del self._addresses[str(previous.ip_address)]
        self._addresses[address] = device_id
        self._devices[device_id] = replacement
        return replacement
