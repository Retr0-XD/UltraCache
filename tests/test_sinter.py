#!/usr/bin/env python3
"""Test Set intersection command (SINTER)."""
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


def test_sinter():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))

    try:
        send_cmd(sock, "AUTH", "tenant_sinter")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        print("✓ AUTH successful")

        for key in ("set:a", "set:b", "set:c", "stringkey"):
            send_cmd(sock, "DEL", key)
            _ = read_resp(sock)

        send_cmd(sock, "SADD", "set:a", "one", "two", "three")
        resp = read_resp(sock)
        assert resp == 3, f"SADD should add 3, got {resp}"

        send_cmd(sock, "SADD", "set:b", "two", "three", "four")
        resp = read_resp(sock)
        assert resp == 3, f"SADD should add 3, got {resp}"

        send_cmd(sock, "SADD", "set:c", "three", "four", "five")
        resp = read_resp(sock)
        assert resp == 3, f"SADD should add 3, got {resp}"

        send_cmd(sock, "SINTER", "set:a", "set:b")
        resp = read_resp(sock)
        assert sorted(resp) == ["three", "two"], f"SINTER should return ['three','two'], got {resp}"
        print("✓ SINTER returns intersection of two sets")

        send_cmd(sock, "SINTER", "set:a", "set:b", "set:c")
        resp = read_resp(sock)
        assert sorted(resp) == ["three"], f"SINTER should return ['three'], got {resp}"
        print("✓ SINTER returns intersection of three sets")

        send_cmd(sock, "SINTER", "set:a", "missing")
        resp = read_resp(sock)
        assert resp == [], f"SINTER with missing key should return [], got {resp}"
        print("✓ SINTER returns empty when key missing")

        send_cmd(sock, "SET", "stringkey", "value")
        resp = read_resp(sock)
        assert resp == "OK", f"SET failed: {resp}"

        send_cmd(sock, "SINTER", "set:a", "stringkey")
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp), f"SINTER on string should return WRONGTYPE, got {resp}"
        print("✓ SINTER on string returns WRONGTYPE")

        print("\n✅ All SINTER tests passed!")
    finally:
        sock.close()


if __name__ == "__main__":
    try:
        test_sinter()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
