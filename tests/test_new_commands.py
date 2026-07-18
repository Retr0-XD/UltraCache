#!/usr/bin/env python3
"""
Integration tests for the new Redis-compatible commands added to UltraCache:
- String: GETSET, STRLEN, SET EX/PX/NX/XX options
- Hash: HINCRBYFLOAT, HSETNX
- Set: SMOVE, SPOP
- Sorted Set: ZINCRBY, ZRANGEBYSCORE, ZREMRANGEBYSCORE, ZCOUNT
- List: LPUSHX, RPUSHX, LINDEX, LSET, LTRIM, LINSERT
- TTL variants: PEXPIRE, PEXPIREAT, EXPIREAT
- Server: DBSIZE, RANDOMKEY, ECHO, INFO, CONFIG GET/SET
"""
import socket
import time

HOST = "127.0.0.1"
PORT = 6379


def send_cmd(sock, *args):
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
    sock.sendall(cmd.encode())


def recv_resp(sock):
    def read_line():
        line = b""
        while True:
            char = sock.recv(1)
            if not char:
                raise ConnectionError("Connection closed")
            line += char
            if line.endswith(b"\r\n"):
                return line[:-2].decode()

    first = read_line()
    if first.startswith("+"):
        return first[1:]
    elif first.startswith("-"):
        return first
    elif first.startswith(":"):
        return int(first[1:])
    elif first.startswith("$"):
        length = int(first[1:])
        if length == -1:
            return None
        data = sock.recv(length)
        sock.recv(2)
        return data.decode()
    elif first.startswith("*"):
        count = int(first[1:])
        if count == -1:
            return None
        return [recv_resp(sock) for _ in range(count)]
    return first


def fresh_sock():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    return s


def test_set_options():
    s = fresh_sock()
    send_cmd(s, "DEL", "optkey")
    recv_resp(s)
    # NX on missing key succeeds
    send_cmd(s, "SET", "optkey", "v1", "NX")
    assert recv_resp(s) == "OK"
    # NX on existing key fails
    send_cmd(s, "SET", "optkey", "v2", "NX")
    assert recv_resp(s) is None
    # XX on existing key succeeds
    send_cmd(s, "SET", "optkey", "v3", "XX")
    assert recv_resp(s) == "OK"
    # XX on missing key fails
    send_cmd(s, "DEL", "optkey2")
    recv_resp(s)
    send_cmd(s, "SET", "optkey2", "x", "XX")
    assert recv_resp(s) is None
    # EX sets expiry
    send_cmd(s, "SET", "optkey3", "y", "EX", "100")
    assert recv_resp(s) == "OK"
    send_cmd(s, "TTL", "optkey3")
    ttl = recv_resp(s)
    assert 1 <= ttl <= 100, f"TTL out of range: {ttl}"
    # PX sets expiry in ms
    send_cmd(s, "SET", "optkey4", "z", "PX", "50000")
    assert recv_resp(s) == "OK"
    send_cmd(s, "PTTL", "optkey4")
    pttl = recv_resp(s)
    assert 1 <= pttl <= 50000, f"PTTL out of range: {pttl}"
    s.close()


def test_getset_strlen():
    s = fresh_sock()
    send_cmd(s, "DEL", "gs")
    recv_resp(s)
    # GETSET on missing key returns nil
    send_cmd(s, "GETSET", "gs", "hello")
    assert recv_resp(s) is None
    send_cmd(s, "GET", "gs")
    assert recv_resp(s) == "hello"
    # STRLEN
    send_cmd(s, "STRLEN", "gs")
    assert recv_resp(s) == 5
    # GETSET replaces and returns old
    send_cmd(s, "GETSET", "gs", "hi")
    assert recv_resp(s) == "hello"
    send_cmd(s, "STRLEN", "gs")
    assert recv_resp(s) == 2
    s.close()


def test_hash_extended():
    s = fresh_sock()
    send_cmd(s, "DEL", "h")
    recv_resp(s)
    # HSETNX on missing field
    send_cmd(s, "HSETNX", "h", "f1", "10")
    assert recv_resp(s) == 1
    # HSETNX on existing field fails
    send_cmd(s, "HSETNX", "h", "f1", "20")
    assert recv_resp(s) == 0
    send_cmd(s, "HGET", "h", "f1")
    assert recv_resp(s) == "10"
    # HINCRBYFLOAT
    send_cmd(s, "HINCRBYFLOAT", "h", "f1", "1.5")
    assert recv_resp(s) == "11.5"
    send_cmd(s, "HINCRBYFLOAT", "h", "f1", "-0.5")
    assert recv_resp(s) == "11"
    s.close()


def test_set_move_pop():
    s = fresh_sock()
    send_cmd(s, "DEL", "src", "dst")
    recv_resp(s)
    send_cmd(s, "SADD", "src", "a", "b", "c")
    recv_resp(s)
    # SMOVE
    send_cmd(s, "SMOVE", "src", "dst", "a")
    assert recv_resp(s) == 1
    send_cmd(s, "SISMEMBER", "src", "a")
    assert recv_resp(s) == 0
    send_cmd(s, "SISMEMBER", "dst", "a")
    assert recv_resp(s) == 1
    # SMOVE non-existing member
    send_cmd(s, "SMOVE", "src", "dst", "zzz")
    assert recv_resp(s) == 0
    # SPOP
    send_cmd(s, "SPOP", "src")
    popped = recv_resp(s)
    assert popped in ("b", "c")
    send_cmd(s, "SCARD", "src")
    assert recv_resp(s) == 1
    s.close()


def test_zset_extended():
    s = fresh_sock()
    send_cmd(s, "DEL", "z")
    recv_resp(s)
    send_cmd(s, "ZADD", "z", "1", "a", "2", "b", "3", "c")
    recv_resp(s)
    # ZINCRBY
    send_cmd(s, "ZINCRBY", "z", "10", "a")
    assert recv_resp(s) == "11"
    # ZCOUNT in range
    send_cmd(s, "ZCOUNT", "z", "2", "13")
    assert recv_resp(s) == 2  # b(2) and a(11)
    # ZRANGEBYSCORE
    send_cmd(s, "ZRANGEBYSCORE", "z", "2", "13")
    assert recv_resp(s) == ["b", "a"]
    # ZRANGEBYSCORE with exclusive bound
    send_cmd(s, "ZRANGEBYSCORE", "z", "(2", "13")
    assert recv_resp(s) == ["a"]
    # ZREMRANGEBYSCORE
    send_cmd(s, "ZREMRANGEBYSCORE", "z", "0", "5")
    recv_resp(s)
    send_cmd(s, "ZCARD", "z")
    assert recv_resp(s) == 1  # only a(11) remains
    s.close()


def test_list_extended():
    s = fresh_sock()
    send_cmd(s, "DEL", "l")
    recv_resp(s)
    send_cmd(s, "RPUSH", "l", "a", "b", "c")
    recv_resp(s)
    # LPUSHX / RPUSHX on existing list
    send_cmd(s, "LPUSHX", "l", "x")
    assert recv_resp(s) == 4
    send_cmd(s, "RPUSHX", "l", "y")
    assert recv_resp(s) == 5
    # LINDEX
    send_cmd(s, "LINDEX", "l", "0")
    assert recv_resp(s) == "x"
    send_cmd(s, "LINDEX", "l", "-1")
    assert recv_resp(s) == "y"
    # LSET
    send_cmd(s, "LSET", "l", "1", "B")
    assert recv_resp(s) == "OK"
    send_cmd(s, "LINDEX", "l", "1")
    assert recv_resp(s) == "B"
    # LINSERT
    send_cmd(s, "LINSERT", "l", "BEFORE", "B", "Z")
    assert recv_resp(s) == 6
    send_cmd(s, "LINDEX", "l", "1")
    assert recv_resp(s) == "Z"
    # LTRIM
    send_cmd(s, "LTRIM", "l", "0", "2")
    assert recv_resp(s) == "OK"
    send_cmd(s, "LLEN", "l")
    assert recv_resp(s) == 3
    # LPUSHX on missing key returns 0
    send_cmd(s, "DEL", "l2")
    recv_resp(s)
    send_cmd(s, "LPUSHX", "l2", "q")
    assert recv_resp(s) == 0
    s.close()


def test_ttl_variants():
    s = fresh_sock()
    send_cmd(s, "DEL", "tk")
    recv_resp(s)
    send_cmd(s, "SET", "tk", "v")
    recv_resp(s)
    # PEXPIRE
    send_cmd(s, "PEXPIRE", "tk", "30000")
    assert recv_resp(s) == 1
    send_cmd(s, "PTTL", "tk")
    pttl = recv_resp(s)
    assert 1 <= pttl <= 30000
    # PEXPIREAT
    ts = int(time.time() * 1000) + 60000
    send_cmd(s, "PEXPIREAT", "tk", str(ts))
    assert recv_resp(s) == 1
    # EXPIREAT
    ts2 = int(time.time()) + 120
    send_cmd(s, "EXPIREAT", "tk", str(ts2))
    assert recv_resp(s) == 1
    send_cmd(s, "TTL", "tk")
    ttl = recv_resp(s)
    assert 1 <= ttl <= 120
    s.close()


def test_server_commands():
    s = fresh_sock()
    send_cmd(s, "DEL", "srv1")
    recv_resp(s)
    send_cmd(s, "SET", "srv1", "v")
    recv_resp(s)
    # DBSIZE
    send_cmd(s, "DBSIZE")
    assert isinstance(recv_resp(s), int)
    # RANDOMKEY
    send_cmd(s, "RANDOMKEY")
    assert recv_resp(s) == "srv1"
    # ECHO
    send_cmd(s, "ECHO", "hello world")
    assert recv_resp(s) == "hello world"
    # INFO
    send_cmd(s, "INFO")
    info = recv_resp(s)
    assert "tenant_id" in info
    # CONFIG GET
    send_cmd(s, "CONFIG", "GET", "maxmemory")
    cfg = recv_resp(s)
    assert isinstance(cfg, list) and len(cfg) == 2
    # CONFIG SET
    send_cmd(s, "CONFIG", "SET", "maxmemory", "67108864")
    assert recv_resp(s) == "OK"
    s.close()


if __name__ == "__main__":
    test_set_options()
    test_getset_strlen()
    test_hash_extended()
    test_set_move_pop()
    test_zset_extended()
    test_list_extended()
    test_ttl_variants()
    test_server_commands()
    print("All new-command integration tests passed.")
