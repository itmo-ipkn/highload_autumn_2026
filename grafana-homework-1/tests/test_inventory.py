import unittest

from pydantic import ValidationError

from app.inventory import AddressConflict, DeviceInput, DeviceNotFound, DeviceRegistry


def device(address="192.0.2.10", **fields):
    return DeviceInput(name="lab-switch", ip_address=address, kind="switch", **fields)


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.registry = DeviceRegistry()

    def test_register_read_and_change_state(self):
        registered = self.registry.create(device())
        self.assertEqual(self.registry.get(registered.id).state, "active")
        changed = self.registry.replace(registered.id, device(state="maintenance"))
        self.assertEqual(changed.id, registered.id)
        self.assertEqual(self.registry.get(registered.id).state, "maintenance")

    def test_duplicate_ipv4_is_rejected(self):
        self.registry.create(device())
        with self.assertRaises(AddressConflict):
            self.registry.create(device())

    def test_equivalent_ipv6_addresses_conflict(self):
        self.registry.create(device("2001:db8::1"))
        with self.assertRaises(AddressConflict):
            self.registry.create(device("2001:0db8:0:0:0:0:0:1"))

    def test_failed_update_preserves_record_and_address_index(self):
        first = self.registry.create(device())
        self.registry.create(device("192.0.2.11"))
        with self.assertRaises(AddressConflict):
            self.registry.replace(first.id, device("192.0.2.11", state="disabled"))
        self.assertEqual(str(self.registry.get(first.id).ip_address), "192.0.2.10")
        self.assertEqual(self.registry.get(first.id).state, "active")
        with self.assertRaises(AddressConflict):
            self.registry.create(device())

    def test_changing_address_releases_previous_address(self):
        first = self.registry.create(device())
        self.registry.replace(first.id, device("192.0.2.12"))
        another = self.registry.create(device())
        self.assertNotEqual(another.id, first.id)

    def test_missing_device(self):
        with self.assertRaises(DeviceNotFound):
            self.registry.get("missing")
        with self.assertRaises(DeviceNotFound):
            self.registry.replace("missing", device())

    def test_validation_rejects_invalid_address_kind_state_and_empty_name(self):
        valid = {"name": "edge", "ip_address": "192.0.2.20", "kind": "router"}
        for field, value in (("ip_address", "not-an-ip"), ("kind", "printer"), ("state", "unknown"), ("name", "  ")):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                DeviceInput(**{**valid, field: value})

    def test_address_uniqueness_is_per_instance(self):
        self.registry.create(device())
        other = DeviceRegistry()
        self.assertEqual(str(other.create(device()).ip_address), "192.0.2.10")


if __name__ == "__main__":
    unittest.main()
