"""Build readable cloud-init using only a public SSH key and the reviewed installer."""
import json
import sys
from pathlib import Path
if len(sys.argv) != 3:
    raise SystemExit("Usage: python3 cloud/render_cloud_init.py public_key output.json")
key = Path(sys.argv[1]).read_text().strip()
if "\n" in key or not key.startswith(("ssh-ed25519 ", "ssh-rsa ", "ecdsa-sha2-")):
    raise SystemExit("Expected exactly one SSH PUBLIC key")
installer = Path(__file__).with_name("install-docker.sh").read_text()
config = {
    "users": [{"name": "lab", "groups": ["sudo"], "shell": "/bin/bash",
               "sudo": ["ALL=(ALL) NOPASSWD:ALL"], "lock_passwd": True,
               "ssh_authorized_keys": [key]}],
    "ssh_pwauth": False,
    "disable_root": True,
    "write_files": [{"path": "/opt/s1/install-docker.sh", "permissions": "0755",
                     "owner": "root:root", "content": installer}],
    "runcmd": [["bash", "/opt/s1/install-docker.sh"]],
}
# JSON is YAML; $$ prevents yc from expanding the laptop environment in user-data.
Path(sys.argv[2]).write_text("#cloud-config\n" + json.dumps(config, indent=2).replace("$", "$$") + "\n")
