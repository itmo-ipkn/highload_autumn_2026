"""Run in the same environment as the application."""
from pathlib import Path
from grpc_tools import protoc

root = Path(__file__).resolve().parents[1]
raise SystemExit(protoc.main([
    "grpc_tools.protoc", f"-I{root / 'proto'}",
    f"--python_out={root / 'app'}", f"--grpc_python_out={root / 'app'}",
    str(root / "proto/catalog.proto"),
]))
