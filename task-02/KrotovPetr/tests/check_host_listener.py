"""Check real Kafka metadata on loopback using the v0 Metadata protocol."""
import socket
import struct


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("Kafka closed the connection before the response ended")
        data.extend(chunk)
    return bytes(data)


def advertised_brokers(port):
    client = b"local-host-check"
    request = struct.pack(">hhih", 3, 0, 42, len(client)) + client
    request += struct.pack(">ih", 1, 6) + b"events"
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(struct.pack(">i", len(request)) + request)
        size = struct.unpack(">i", receive(sock, 4))[0]
        if not 0 < size < 4 * 1024 * 1024:
            raise ValueError(f"Invalid Kafka response size {size}")
        response = receive(sock, size)
    correlation, count = struct.unpack_from(">ii", response)
    if correlation != 42 or count != 3:
        raise ValueError("Expected a Metadata response describing three brokers")
    position = 8
    brokers = {}
    for _ in range(count):
        node, length = struct.unpack_from(">ih", response, position)
        position += 6
        host = response[position:position + length].decode()
        position += length
        broker_port = struct.unpack_from(">i", response, position)[0]
        position += 4
        brokers[node] = (host, broker_port)
    return brokers


if __name__ == "__main__":
    expected = {n: ("127.0.0.1", n * 10000 + 9092) for n in range(1, 4)}
    for port in (19092, 29092, 39092):
        actual = advertised_brokers(port)
        if actual != expected:
            raise SystemExit(f"Incorrect advertised listeners on {port}: {actual}")
        print(f"127.0.0.1:{port}: all three advertised broker addresses are reachable from host")
