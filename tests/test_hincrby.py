#!/usr/bin/env python3
"""Test Hash increment command (HINCRBY)."""
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


def test_hincrby():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))

    try:
        send_cmd(sock, "AUTH", "tenant_hincr")
        resp = read_resp(sock)
        assert resp == "OK", f"AUTH failed: {resp}"
        print("✓ AUTH successful")

        send_cmd(sock, "DEL", "counter")
        _ = read_resp(sock)

        send_cmd(sock, "HINCRBY", "counter", "views", 1)
        resp = read_resp(sock)
        assert resp == 1, f"HINCRBY should return 1, got {resp}"
        print("✓ HINCRBY creates field")

        send_cmd(sock, "HINCRBY", "counter", "views", 4)
        resp = read_resp(sock)
        assert resp == 5, f"HINCRBY should return 5, got {resp}"
        print("✓ HINCRBY increments")

        send_cmd(sock, "HGET", "counter", "views")
        resp = read_resp(sock)
        assert resp == "5", f"HGET should return '5', got {resp}"
        print("✓ HGET reflects incremented value")

        send_cmd(sock, "HSET", "counter", "bad", "oops")
        resp = read_resp(sock)
        assert resp in (0, 1), f"HSET failed: {resp}"

        send_cmd(sock, "HINCRBY", "counter", "bad", 1)
        resp = read_resp(sock)
        assert "ERROR" in str(resp), f"HINCRBY should fail on non-integer, got {resp}"
        print("✓ HINCRBY fails on non-integer")

        send_cmd(sock, "SET", "stringkey", "value")
        resp = read_resp(sock)
        assert resp == "OK", f"SET failed: {resp}"

        send_cmd(sock, "HINCRBY", "stringkey", "field", 1)
        resp = read_resp(sock)
        assert "WRONGTYPE" in str(resp), f"HINCRBY on string should return WRONGTYPE, got {resp}"
        print("✓ HINCRBY on string returns WRONGTYPE")

        print("\n✅ All HINCRBY tests passed!")
    finally:
        sock.close()


if __name__ == "__main__":
    try:
        test_hincrby()
    except Exception as e:
        print(f"\n❌ Test failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
