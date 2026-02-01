#!/usr/bin/env python3
"""
Test extended Hash commands: HGETALL, HKEYS, HVALS
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

def test_hgetall():
    """Test HGETALL command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "myhash")
    recv_resp(sock)
    
    # Set multiple fields
    send_cmd(sock, "HSET", "myhash", "field1", "value1")
    recv_resp(sock)
    send_cmd(sock, "HSET", "myhash", "field2", "value2")
    recv_resp(sock)
    send_cmd(sock, "HSET", "myhash", "field3", "value3")
    recv_resp(sock)
    
    # Get all
    send_cmd(sock, "HGETALL", "myhash")
    resp = recv_resp(sock)
    assert len(resp) == 6, f"Expected 6 elements, got {len(resp)}"
    
    # Convert to dict
    hash_dict = {resp[i]: resp[i+1] for i in range(0, len(resp), 2)}
    assert hash_dict == {"field1": "value1", "field2": "value2", "field3": "value3"}
    
    # Empty hash
    send_cmd(sock, "HGETALL", "nonexistent")
    resp = recv_resp(sock)
    assert resp == [], f"Expected [], got {resp}"
    
    sock.close()
    print("✓ HGETALL works correctly")

def test_hkeys():
    """Test HKEYS command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "myhash")
    recv_resp(sock)
    
    send_cmd(sock, "HSET", "myhash", "name", "Alice")
    recv_resp(sock)
    send_cmd(sock, "HSET", "myhash", "age", "30")
    recv_resp(sock)
    send_cmd(sock, "HSET", "myhash", "city", "NYC")
    recv_resp(sock)
    
    send_cmd(sock, "HKEYS", "myhash")
    resp = recv_resp(sock)
    assert set(resp) == {"name", "age", "city"}, f"Expected {{name, age, city}}, got {resp}"
    
    # Empty hash
    send_cmd(sock, "HKEYS", "nonexistent")
    resp = recv_resp(sock)
    assert resp == [], f"Expected [], got {resp}"
    
    sock.close()
    print("✓ HKEYS works correctly")

def test_hvals():
    """Test HVALS command."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", 6379))
    
    send_cmd(sock, "DEL", "myhash")
    recv_resp(sock)
    
    send_cmd(sock, "HSET", "myhash", "k1", "v1")
    recv_resp(sock)
    send_cmd(sock, "HSET", "myhash", "k2", "v2")
    recv_resp(sock)
    send_cmd(sock, "HSET", "myhash", "k3", "v3")
    recv_resp(sock)
    
    send_cmd(sock, "HVALS", "myhash")
    resp = recv_resp(sock)
    assert set(resp) == {"v1", "v2", "v3"}, f"Expected {{v1, v2, v3}}, got {resp}"
    
    # Empty hash
    send_cmd(sock, "HVALS", "nonexistent")
    resp = recv_resp(sock)
    assert resp == [], f"Expected [], got {resp}"
    
    sock.close()
    print("✓ HVALS works correctly")

if __name__ == "__main__":
    try:
        test_hgetall()
        test_hkeys()
        test_hvals()
        print("\n✅ All extended Hash tests passed!")
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
