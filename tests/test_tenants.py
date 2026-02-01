#!/usr/bin/env python3
"""Test TENANTS admin command."""
import socket
import sys


def send_cmd(sock, *args):
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_bytes = str(arg).encode("utf-8")
        cmd += f"${len(arg_bytes)}\r\n{arg_bytes.decode('utf-8')}\r\n"
    sock.sendall(cmd.encode("utf-8"))


def read_resp(sock):
    first_byte = sock.recv(1)
    if not first_byte:
        return None

    if first_byte == b"+":
        line = b""
        while not line.endswith(b"\r\n"):
            line += sock.recv(1)
        return line[:-2].decode("utf-8")
    if first_byte == b"-":
        line = b""
        while not line.endswith(b"\r\n"):
            line += sock.recv(1)
        return f"ERROR: {line[:-2].decode('utf-8')}"
    if first_byte == b":":
        line = b""
        while not line.endswith(b"\r\n"):
            line += sock.recv(1)
        return int(line[:-2].decode("utf-8"))
    if first_byte == b"$":
        line = b""
        while not line.endswith(b"\r\n"):
            line += sock.recv(1)
        length = int(line[:-2].decode("utf-8"))
        if length == -1:
            return None
        data = sock.recv(length)
        sock.recv(2)
        return data.decode("utf-8")
    if first_byte == b"*":
        line = b""
        while not line.endswith(b"\r\n"):
            line += sock.recv(1)
        count = int(line[:-2].decode("utf-8"))
        result = []
        for _ in range(count):
            result.append(read_resp(sock))
        return result

    return None


def test_tenants():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))

    try:
        send_cmd(sock, "AUTH", "tenant_a")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        send_cmd(sock, "AUTH", "tenant_b")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"

        send_cmd(sock, "TENANTS")
        resp = read_resp(sock)
        assert isinstance(resp, list), f"TENANTS should return array, got {resp}"

        joined = "\n".join(resp)
        assert "id=default" in joined, f"default tenant missing: {resp}"
        assert "id=tenant_a" in joined, f"tenant_a missing: {resp}"
        assert "id=tenant_b" in joined, f"tenant_b missing: {resp}"
        print("✓ TENANTS returns default and auth tenants")

        print("\n✅ All TENANTS tests passed!")
    finally:
        sock.close()


if __name__ == "__main__":
    try:
        test_tenants()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
