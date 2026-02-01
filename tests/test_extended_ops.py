#!/usr/bin/env python3
"""
Test extended Set and ZSet commands: SCARD, SISMEMBER, ZCARD, ZSCORE
"""
import socket
import sys

def send_cmd(sock, *args):
    """Send a RESP command."""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        cmd += f"${len(arg_str)}\r\n{arg_str}\r\n"
    sock.sendall(cmd.encode())

def recv_resp(sock):
    """Receive a RESP response."""
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
        sock.recv(2)  # \r\n
        return data.decode()
    elif first.startswith("*"):
        count = int(first[1:])
        if count == -1:
            return None
        return [recv_resp(sock) for _ in range(count)]
    return first

def test_scard():
    """Test SCARD command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "myset")
    recv_resp(sock)
    
    # Empty set
    send_cmd(sock, "SCARD", "myset")
    resp = recv_resp(sock)
    assert resp == 0, f"Expected 0, got {resp}"
    
    # Add members
    send_cmd(sock, "SADD", "myset", "a")
    recv_resp(sock)
    send_cmd(sock, "SADD", "myset", "b")
    recv_resp(sock)
    send_cmd(sock, "SADD", "myset", "c")
    recv_resp(sock)
    
    send_cmd(sock, "SCARD", "myset")
    resp = recv_resp(sock)
    assert resp == 3, f"Expected 3, got {resp}"
    
    # Add duplicate (no change)
    send_cmd(sock, "SADD", "myset", "a")
    recv_resp(sock)
    
    send_cmd(sock, "SCARD", "myset")
    resp = recv_resp(sock)
    assert resp == 3, f"Expected 3, got {resp}"
    
    sock.close()
    print("✓ SCARD works correctly")

def test_sismember():
    """Test SISMEMBER command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "myset")
    recv_resp(sock)
    
    send_cmd(sock, "SADD", "myset", "apple")
    recv_resp(sock)
    send_cmd(sock, "SADD", "myset", "banana")
    recv_resp(sock)
    
    # Member exists
    send_cmd(sock, "SISMEMBER", "myset", "apple")
    resp = recv_resp(sock)
    assert resp == 1, f"Expected 1, got {resp}"
    
    # Member doesn't exist
    send_cmd(sock, "SISMEMBER", "myset", "orange")
    resp = recv_resp(sock)
    assert resp == 0, f"Expected 0, got {resp}"
    
    # Non-existent set
    send_cmd(sock, "SISMEMBER", "noset", "apple")
    resp = recv_resp(sock)
    assert resp == 0, f"Expected 0, got {resp}"
    
    sock.close()
    print("✓ SISMEMBER works correctly")

def test_zcard():
    """Test ZCARD command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "myzset")
    recv_resp(sock)
    
    # Empty zset
    send_cmd(sock, "ZCARD", "myzset")
    resp = recv_resp(sock)
    assert resp == 0, f"Expected 0, got {resp}"
    
    # Add members
    send_cmd(sock, "ZADD", "myzset", "1.0", "one")
    recv_resp(sock)
    send_cmd(sock, "ZADD", "myzset", "2.0", "two")
    recv_resp(sock)
    send_cmd(sock, "ZADD", "myzset", "3.0", "three")
    recv_resp(sock)
    
    send_cmd(sock, "ZCARD", "myzset")
    resp = recv_resp(sock)
    assert resp == 3, f"Expected 3, got {resp}"
    
    sock.close()
    print("✓ ZCARD works correctly")

def test_zscore():
    """Test ZSCORE command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "myzset")
    recv_resp(sock)
    
    send_cmd(sock, "ZADD", "myzset", "1.5", "member1")
    recv_resp(sock)
    send_cmd(sock, "ZADD", "myzset", "2.7", "member2")
    recv_resp(sock)
    
    # Get score
    send_cmd(sock, "ZSCORE", "myzset", "member1")
    resp = recv_resp(sock)
    assert float(resp) == 1.5, f"Expected 1.5, got {resp}"
    
    send_cmd(sock, "ZSCORE", "myzset", "member2")
    resp = recv_resp(sock)
    assert float(resp) == 2.7, f"Expected 2.7, got {resp}"
    
    # Non-existent member
    send_cmd(sock, "ZSCORE", "myzset", "nonexistent")
    resp = recv_resp(sock)
    assert resp is None, f"Expected None, got {resp}"
    
    # Non-existent zset
    send_cmd(sock, "ZSCORE", "nozset", "member")
    resp = recv_resp(sock)
    assert resp is None, f"Expected None, got {resp}"
    
    sock.close()
    print("✓ ZSCORE works correctly")

if __name__ == "__main__":
    try:
        test_scard()
        test_sismember()
        test_zcard()
        test_zscore()
        print("\n✅ All extended Set and ZSet tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
