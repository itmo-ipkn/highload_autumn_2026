"""Загрузить проверенный публичный образ YTsaurus, если docker pull вернул denied."""
import hashlib
import io
import json
import subprocess
import tarfile
from time import time_ns
from urllib.request import Request, urlopen

REGISTRY = "https://ghcr.io"
REPOSITORY = "ytsaurus/local"
TAG = "ghcr.io/ytsaurus/local:stable-24.2"
DIGEST = "sha256:5a54a32c4ad520f0742004d3b669b9ea9069c488223cccea2e0bf8af5c96d66f"
IMAGE_ID = "sha256:d18f9e22b5541acff7fe1586673240ee52a0b9f34b41fb2adb05bcc0dcbb5fc3"


class VerifiedReader:
    def __init__(self, raw):
        self.raw = raw
        self.digest = hashlib.sha256()

    def read(self, n):
        data = self.raw.read(n)
        self.digest.update(data)
        return data


def load():
    cached = subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", TAG],
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    if cached.returncode == 0 and cached.stdout.strip() == IMAGE_ID:
        print(f"cached {TAG}")
        return
    with urlopen(f"{REGISTRY}/token?scope=repository:{REPOSITORY}:pull", timeout=45) as response:
        token = json.load(response)["token"]

    def get(path):
        url = f"{REGISTRY}/v2/{REPOSITORY}/{path}?download={time_ns()}"
        headers = {"Authorization": "Bearer " + token, "Accept": "application/vnd.docker.distribution.manifest.v2+json"}
        return urlopen(Request(url, headers=headers), timeout=120)

    with get("manifests/" + DIGEST) as response:
        data = response.read()
    if "sha256:" + hashlib.sha256(data).hexdigest() != DIGEST:
        raise ValueError("Неверная контрольная сумма манифеста")
    manifest = json.loads(data)
    if manifest["config"]["digest"] != IMAGE_ID:
        raise ValueError("Не совпадает идентификатор сборки")
    objects = [manifest["config"]] + manifest["layers"]
    paths = ["blobs/sha256/" + obj["digest"][7:] for obj in objects]
    index = json.dumps([{"Config": paths[0], "RepoTags": [TAG], "Layers": paths[1:]}]).encode()
    print(f"download {TAG}", flush=True)
    with subprocess.Popen(["docker", "load"], stdin=subprocess.PIPE) as proc:
        try:
            with tarfile.open(mode="w|", fileobj=proc.stdin) as stream:
                info = tarfile.TarInfo("manifest.json")
                info.size = len(index)
                stream.addfile(info, io.BytesIO(index))
                for path, obj in zip(paths, objects):
                    info = tarfile.TarInfo(path)
                    info.size = obj["size"]
                    with get("blobs/" + obj["digest"]) as response:
                        reader = VerifiedReader(response)
                        stream.addfile(info, reader)
                        if "sha256:" + reader.digest.hexdigest() != obj["digest"]:
                            raise ValueError("Неверная контрольная сумма слоя")
        finally:
            proc.stdin.close()
        if proc.wait() != 0:
            raise SystemExit("docker load завершился ошибкой")


if __name__ == "__main__":
    load()
